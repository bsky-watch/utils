package listserver

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/Jille/convreq"
	"github.com/Jille/convreq/respond"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/xrpc"
	cborgen "github.com/whyrusleeping/cbor-gen"

	"bsky.watch/utils/aturl"
	"bsky.watch/utils/didset"
	"bsky.watch/utils/firehose"
	"bsky.watch/utils/pagination"
)

type Request struct {
	Subject string
}

type Response struct {
	Subject string                               `json:"subject"`
	Results map[string]ResponseFromSingleAccount `json:"results"`

	Listitems map[string]string `json:"listitems,omitempty"` // Deprecated
	Rev       string            `json:"rev,omitempty"`       // Deprecated
	Cid       string            `json:"cid,omitempty"`       // Deprecated
}

type ResponseFromSingleAccount struct {
	Listitems map[string]string `json:"listitems"`
	Rev       string            `json:"rev"`
	Cid       string            `json:"cid"`
}

type entry struct {
	List    string
	Subject string
}

// Server keeps track of all `app.bsky.graph.listitem`s for a given account,
// and responds to membership queries over HTTP.
//
// To ensure that you're not missing any records, you need to connect to firehose
// *before* calling Sync().
type Server struct {
	client *xrpc.Client

	handler http.HandlerFunc

	// Not supposed to change after initialization, so no need for locking.
	state map[string]*accountState

	mu                   sync.Mutex
	firehoseHookReturned bool
}

type accountState struct {
	sync.RWMutex
	lastKnownRev string
	lastCid      string
	// Don't try to do anything smart yet, just iterating over the map
	// should be fast enough for now.
	rkeyToEntry map[string]entry
	initQueue   []*comatproto.SyncSubscribeRepos_Commit
}

func New(client *xrpc.Client, did ...string) *Server {
	s := &Server{
		client: client,
		state:  map[string]*accountState{},
	}
	for _, d := range did {
		s.state[d] = &accountState{}
	}
	s.handler = convreq.Wrap(s.serveHTTP)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.handler(w, req)
}

func (s *Server) serveHTTP(ctx context.Context, req *http.Request) convreq.HttpResponse {
	var input Request
	input.Subject = req.FormValue("subject")
	if input.Subject == "" {
		return respond.BadRequest("missing 'subject'")
	}

	result := &Response{
		Subject: input.Subject,
		Results: map[string]ResponseFromSingleAccount{},
	}

	for did, state := range s.state {
		r := ResponseFromSingleAccount{
			Listitems: map[string]string{},
		}

		state.RLock()
		if state.lastKnownRev == "" {
			state.RUnlock()
			return respond.TooEarly("server not ready yet")
		}
		for rkey, entry := range state.rkeyToEntry {
			if entry.Subject == input.Subject {
				result.Listitems[rkey] = entry.List
			}
		}
		r.Rev = state.lastKnownRev
		r.Cid = state.lastCid
		state.RUnlock()

		result.Results[did] = r

		// Backwards compatibility.
		if len(s.state) == 1 {
			result.Listitems = r.Listitems
			result.Rev = r.Rev
			result.Cid = r.Cid
		}
	}

	return respond.JSON(result)
}

func (s *Server) UpdateFromFirehose() firehose.Hook {
	s.mu.Lock()
	s.firehoseHookReturned = true
	s.mu.Unlock()

	predicates := []firehose.Predicate{}
	for did := range s.state {
		predicates = append(predicates, firehose.From(did))
	}

	return firehose.Hook{
		CallPerCommit: true,
		Predicate:     firehose.AnyOf(predicates...),
		Action: func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, _ *comatproto.SyncSubscribeRepos_RepoOp, _ cborgen.CBORMarshaler) {
			log := zerolog.Ctx(ctx)

			server := s
			s := server.state[commit.Repo]
			s.Lock()
			startingUp := false
			alreadyProcessed := false
			missingPreviousCommit := false
			if s.lastKnownRev == "" {
				startingUp = true
				s.initQueue = append(s.initQueue, commit)
			} else if commit.Rev == s.lastKnownRev {
				alreadyProcessed = true
			} else if commit.Since != nil && *commit.Since != s.lastKnownRev {
				missingPreviousCommit = true
			}
			s.Unlock()
			if startingUp || alreadyProcessed {
				return
			}
			if missingPreviousCommit {
				log.Error().Msgf("We somehow missed the previous commit %q. List memberships may be inaccurate.", *commit.Since)
			}

			if err := server.processCommit(ctx, commit, false); err != nil {
				log.Error().Err(err).Msgf("Failed to process commit %q: %s", commit.Rev, err)
				return
			}
		},
	}
}

func (s *Server) processCommit(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, locked bool) error {
	insert := map[string]entry{}
	remove := []string{}

	err := func() error {
		var err error
		var repo_ *repo.Repo
		if len(commit.Blocks) > 0 {
			repo_, err = repo.ReadRepoFromCar(ctx, bytes.NewReader(commit.Blocks))
			if err != nil {
				return fmt.Errorf("ReadRepoFromCar: %w", err)
			}
		}

		for _, op := range commit.Ops {
			parts := strings.SplitN(op.Path, "/", 2)
			if len(parts) != 2 {
				log.Error().Msgf("Unexpected number of parts in op.Path: %q", op.Path)
				continue
			}
			if parts[0] != "app.bsky.graph.listitem" {
				continue
			}
			rkey := parts[1]

			switch op.Action {
			case "create", "update":
				if repo_ == nil {
					return fmt.Errorf("%q op without record data (seq=%d)", op.Action, commit.Seq)
				}

				_, rec, err := repo_.GetRecord(ctx, op.Path)
				if err != nil {
					return fmt.Errorf("failed to get record %q: %w", op.Path, err)
				}

				switch record := rec.(type) {
				case *bsky.GraphListitem:
					insert[rkey] = entry{
						Subject: record.Subject,
						List:    record.List,
					}
				default:
					return fmt.Errorf("unexpected record type %T", record)
				}
			case "delete":
				remove = append(remove, rkey)
			default:
				return fmt.Errorf("unknown action %q", op.Action)
			}
		}
		return nil
	}()

	log := zerolog.Ctx(ctx)
	log.Debug().Msgf("Adding: %+v, removing: %+v", insert, remove)

	state := s.state[commit.Repo]
	if !locked {
		s.mu.Lock()
	}
	for _, rkey := range remove {
		delete(state.rkeyToEntry, rkey)
	}
	for k, v := range insert {
		state.rkeyToEntry[k] = v
	}
	if commit.Rev > state.lastKnownRev {
		state.lastKnownRev = commit.Rev
		state.lastCid = commit.Commit.String()
	}
	if !locked {
		s.mu.Unlock()
	}

	return err
}

func (s *Server) Sync(ctx context.Context) error {
	s.mu.Lock()
	ok := s.firehoseHookReturned
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("must connect to firehose before calling Sync()")
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(s.state))

	for did := range s.state {
		wg.Add(1)
		go func(did string) {
			defer wg.Done()

			errCh <- s.syncSingleAccount(ctx, did)
		}(did)
	}

	wg.Wait()

	errs := []error{}
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Server) syncSingleAccount(ctx context.Context, did string) error {
	status, err := comatproto.SyncGetLatestCommit(ctx, s.client, did)
	if err != nil {
		return fmt.Errorf("com.atproto.sync.getLatestCommit: %w", err)
	}

	if status.Rev == "" {
		return fmt.Errorf("server did not return the current `rev` value")
	}

	// Changes happening after status.Rev might affect the result, but
	// that's fine: duplicate mutations in s.initQueue will have no effect.
	entries, err := pagination.Reduce(
		// fetch
		func(cursor string) (*comatproto.RepoListRecords_Output, string, error) {
			resp, err := comatproto.RepoListRecords(ctx, s.client, "app.bsky.graph.listitem", cursor, 100, did, false, "", "")
			cursor = ""
			if resp != nil && resp.Cursor != nil {
				cursor = *resp.Cursor
			}
			return resp, cursor, err
		},
		// combine
		func(resp *comatproto.RepoListRecords_Output, acc map[string]entry) (map[string]entry, error) {
			if acc == nil {
				acc = map[string]entry{}
			}
			for _, rec := range resp.Records {
				switch record := rec.Value.Val.(type) {
				case *bsky.GraphListitem:
					u, err := aturl.Parse(rec.Uri)
					if err != nil {
						return nil, fmt.Errorf("failed to parse %q: %w", rec.Uri, err)
					}
					parts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
					if len(parts) < 2 {
						return nil, fmt.Errorf("invalid URI %q", rec.Uri)
					}
					rkey := parts[1]

					acc[rkey] = entry{
						Subject: record.Subject,
						List:    record.List,
					}
				}
			}
			return acc, nil
		})
	if err != nil {
		return fmt.Errorf("failed to fetch listitems: %w", err)
	}

	state := s.state[did]
	state.Lock()
	state.rkeyToEntry = entries
	state.lastKnownRev = status.Rev
	state.lastCid = status.Cid

	slices.SortFunc(state.initQueue, func(a, b *comatproto.SyncSubscribeRepos_Commit) int {
		return cmp.Compare(a.Rev, b.Rev)
	})
	for _, commit := range state.initQueue {
		if commit.Rev <= state.lastKnownRev {
			continue
		}
		if err := s.processCommit(ctx, commit, true); err != nil {
			return err
		}
	}
	state.initQueue = nil

	s.mu.Unlock()

	return nil
}

func (s *Server) List(uri string) (didset.QueryableDIDSet, error) {
	u, err := aturl.Parse(uri)
	if err != nil {
		return nil, err
	}
	if s.state[u.Host] == nil {
		return nil, fmt.Errorf("%q is not tracked by this server", u.Host)
	}
	return &didSet{uri: uri, state: s.state[u.Host]}, nil
}

type didSet struct {
	uri   string
	state *accountState
}

func (s *didSet) Contains(ctx context.Context, did string) (bool, error) {
	s.state.RLock()
	defer s.state.RUnlock()
	if s.state.lastKnownRev == "" {
		return false, fmt.Errorf("server not ready yet")
	}

	for _, entry := range s.state.rkeyToEntry {
		if entry.List == s.uri {
			return true, nil
		}
	}
	return false, nil
}

func (s *didSet) GetDIDs(ctx context.Context) (didset.StringSet, error) {
	s.state.RLock()
	defer s.state.RUnlock()
	if s.state.lastKnownRev == "" {
		return nil, fmt.Errorf("server not ready yet")
	}

	r := didset.StringSet{}
	for _, entry := range s.state.rkeyToEntry {
		if entry.List == s.uri {
			r[entry.Subject] = true
		}
	}
	return r, nil
}
