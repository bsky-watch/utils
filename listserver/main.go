package listserver

import (
	"bytes"
	"cmp"
	"context"
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
	Subject   string            `json:"subject"`
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
	did    string

	handler http.HandlerFunc

	mu                   sync.RWMutex
	firehoseHookReturned bool
	lastKnownRev         string
	lastCid              string
	// Don't try to do anything smart yet, just iterating over the map
	// should be fast enough for now.
	rkeyToEntry map[string]entry
	initQueue   []*comatproto.SyncSubscribeRepos_Commit
}

func New(client *xrpc.Client, did string) *Server {
	s := &Server{
		client: client,
		did:    did,
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
		Subject:   input.Subject,
		Listitems: map[string]string{},
	}

	s.mu.RLock()
	if s.lastKnownRev == "" {
		s.mu.RUnlock()
		return respond.TooEarly("server not ready yet")
	}
	for rkey, entry := range s.rkeyToEntry {
		if entry.Subject == input.Subject {
			result.Listitems[rkey] = entry.List
		}
	}
	result.Rev = s.lastKnownRev
	result.Cid = s.lastCid
	s.mu.RUnlock()

	return respond.JSON(result)
}

func (s *Server) UpdateFromFirehose() firehose.Hook {
	s.mu.Lock()
	s.firehoseHookReturned = true
	s.mu.Unlock()

	return firehose.Hook{
		Predicate: firehose.From(s.did),
		Action: func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, _ *comatproto.SyncSubscribeRepos_RepoOp, _ cborgen.CBORMarshaler) {
			log := zerolog.Ctx(ctx)
			s.mu.Lock()
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
			s.mu.Unlock()
			if startingUp || alreadyProcessed {
				return
			}
			if missingPreviousCommit {
				log.Error().Msgf("We somehow missed the previous commit %q. List memberships may be inaccurate.", *commit.Since)
			}

			if err := s.processCommit(ctx, commit, false); err != nil {
				log.Error().Err(err).Msgf("Failed to process commit %q: %s", commit.Rev, err)
				return
			}
		},
	}
}

func (s *Server) processCommit(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, locked bool) error {
	repo_, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(commit.Blocks))
	if err != nil {
		return fmt.Errorf("ReadRepoFromCar: %w", err)
	}

	insert := map[string]entry{}
	remove := []string{}
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

	if !locked {
		s.mu.Lock()
	}
	for _, rkey := range remove {
		delete(s.rkeyToEntry, rkey)
	}
	for k, v := range insert {
		s.rkeyToEntry[k] = v
	}
	if commit.Rev > s.lastKnownRev {
		s.lastKnownRev = commit.Rev
		s.lastCid = commit.Commit.String()
	}
	if !locked {
		s.mu.Unlock()
	}

	return nil
}

func (s *Server) Sync(ctx context.Context) error {
	s.mu.Lock()
	ok := s.firehoseHookReturned
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("must connect to firehose before calling Sync()")
	}

	status, err := comatproto.SyncGetLatestCommit(ctx, s.client, s.did)
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
			resp, err := comatproto.RepoListRecords(ctx, s.client, "app.bsky.graph.listitem", cursor, 100, s.did, false, "", "")
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

	s.mu.Lock()
	s.rkeyToEntry = entries
	s.lastKnownRev = status.Rev
	s.lastCid = status.Cid

	slices.SortFunc(s.initQueue, func(a, b *comatproto.SyncSubscribeRepos_Commit) int {
		return cmp.Compare(a.Rev, b.Rev)
	})
	for _, commit := range s.initQueue {
		if commit.Rev <= s.lastKnownRev {
			continue
		}
		if err := s.processCommit(ctx, commit, true); err != nil {
			return err
		}
	}
	s.initQueue = nil

	s.mu.Unlock()

	return nil
}

func (s *Server) List(uri string) (didset.QueryableDIDSet, error) {
	u, err := aturl.Parse(uri)
	if err != nil {
		return nil, err
	}
	if u.Host != s.did {
		return nil, fmt.Errorf("the list does not belong to %q", s.did)
	}
	return &didSet{uri: uri, server: s}, nil
}

type didSet struct {
	uri    string
	server *Server
}

func (s *didSet) Contains(ctx context.Context, did string) (bool, error) {
	s.server.mu.RLock()
	defer s.server.mu.RUnlock()
	if s.server.lastKnownRev == "" {
		return false, fmt.Errorf("server not ready yet")
	}

	for _, entry := range s.server.rkeyToEntry {
		if entry.List == s.uri {
			return true, nil
		}
	}
	return false, nil
}

func (s *didSet) GetDIDs(ctx context.Context) (didset.StringSet, error) {
	s.server.mu.RLock()
	defer s.server.mu.RUnlock()
	if s.server.lastKnownRev == "" {
		return nil, fmt.Errorf("server not ready yet")
	}

	r := didset.StringSet{}
	for _, entry := range s.server.rkeyToEntry {
		if entry.List == s.uri {
			r[entry.Subject] = true
		}
	}
	return r, nil
}
