package firehose

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
	"github.com/rs/zerolog"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
	cbg "github.com/whyrusleeping/cbor-gen"
)

const bufferSize = 1024

type Firehose struct {
	Hooks    []Hook
	Host     string
	StartSeq int64

	seq   int64
	ident string
}

type Predicate func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler) bool

type Hook struct {
	Name          string
	Predicate     Predicate
	Action        func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler)
	OnCursorReset func(ctx context.Context)

	// CallPerCommit specifies if the hook expects to be called once per incoming commit.
	// By default a commit is unwrapped and the hook is called for each Op. If this flag
	// is set, then the hook is called just with the commit struct, op and record are not
	// provided.
	//
	// Note that predicates that look at the op or record will not work properly.
	CallPerCommit bool
}

type event struct {
	Commit      *comatproto.SyncSubscribeRepos_Commit
	CursorReset bool
}

func New() *Firehose {
	return &Firehose{ident: "bsky.watch/utils/firehose"}
}

func (f *Firehose) Run(ctx context.Context) error {

	log := zerolog.Ctx(ctx).With().Str("module", "firehose").Logger()
	ctx, cancel := context.WithCancel(log.WithContext(ctx))
	defer cancel()

	if f.Host == "" {
		f.Host = "bsky.network"
	}
	if f.StartSeq > 0 {
		f.seq = f.StartSeq
	}

	channels := []chan event{}
	for i, h := range f.Hooks {
		ch := make(chan event, bufferSize)
		channels = append(channels, ch)
		go f.runHook(log.With().Int("hook", i).Logger().WithContext(ctx), ch, h)
	}

	for {
		addr, err := url.Parse("wss://host/xrpc/com.atproto.sync.subscribeRepos")
		if err != nil {
			return err
		}
		addr.Host = f.Host
		if f.seq > 0 {
			q := addr.Query()
			q.Add("cursor", fmt.Sprint(f.seq))
			addr.RawQuery = q.Encode()
		}
		conn, _, err := websocket.DefaultDialer.Dial(addr.String(), http.Header{})
		if err != nil {
			log.Error().Err(err).Msgf("websocket dial error")
			time.Sleep(5 * time.Second)
			continue
		}

		heartbeat := f.closeIfIdle(ctx, conn)
		sampler := &zerolog.BurstSampler{Burst: 1, Period: time.Minute}

		callbacks := &events.RepoStreamCallbacks{
			RepoCommit: func(e *comatproto.SyncSubscribeRepos_Commit) error {
				f.seq = e.Seq
				select {
				case heartbeat <- struct{}{}:
				default:
				}
				for i, ch := range channels {
					select {
					case ch <- event{Commit: e}:
					default:
						log := log.Sample(sampler)
						log.Warn().Int("hook", i).Msgf("Hook %d (%s) queue full", i, f.Hooks[i].Name)
					}
				}
				return nil
			},
			RepoInfo: func(e *comatproto.SyncSubscribeRepos_Info) error {
				select {
				case heartbeat <- struct{}{}:
				default:
				}
				for i, ch := range channels {
					if f.Hooks[i].OnCursorReset == nil {
						continue
					}
					// Send synchronously.
					ch <- event{CursorReset: true}
				}

				return nil
			},
		}

		if err := events.HandleRepoStream(ctx, conn, sequential.NewScheduler(f.ident, callbacks.EventHandler)); err != nil {
			log.Error().Err(err).Msgf("HandleRepoStream error")
			conn.Close()
			if ctx.Err() != nil {
				break
			}
		}
		time.Sleep(5 * time.Second)
		log.Debug().Msgf("Restarting HandleRepoStream")
	}

	return ctx.Err()
}

func (f *Firehose) runHook(ctx context.Context, ch chan event, hook Hook) {
	log := zerolog.Ctx(ctx)

	var wg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case e := <-ch:
			switch {
			case e.Commit != nil:
				commit := e.Commit

				if hook.Action == nil {
					continue
				}

				log := log.With().
					Int64("seq", commit.Seq).
					Bool("rebase", commit.Rebase).
					Bool("tooBig", commit.TooBig).
					Str("commit_time", commit.Time).
					Str("repo", commit.Repo).
					Str("commit", commit.Commit.String()).
					Str("rev", commit.Rev).
					Logger()

				wg.Add(1)
				func() {
					defer wg.Done()
					ctx := log.WithContext(ctx)

					defer func() {
						if err := recover(); err != nil {
							log.Error().Msgf("RepoCommit callback has panicked: %+v", err)
						}
					}()

					if hook.CallPerCommit {
						if hook.Predicate == nil || hook.Predicate(ctx, commit, &comatproto.SyncSubscribeRepos_RepoOp{Action: "commit"}, nil) {
							hook.Action(ctx, commit, nil, nil)
						}
						return
					}

					if len(commit.Blocks) == 0 {
						// TODO: allow for the hook to be invoked in this case.
						return
					}
					repo_, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(commit.Blocks))
					if err != nil {
						log.Error().Err(err).Msgf("ReadRepoFromCar: %s", err)
						return
					}
					for _, op := range commit.Ops {
						log.Trace().Interface("op", op).Msg("Op")
						collection := strings.Split(op.Path, "/")[0]
						rcid, rec, err := repo_.GetRecord(ctx, op.Path)
						if err != nil {
							log.Trace().Err(err).Msgf("GetRecord(%q)", op.Path)

							log.Trace().Msgf("Signed commit: %+v", repo_.SignedCommit())
							repo_.ForEach(ctx, collection, func(k string, v cid.Cid) error {
								log.Trace().Msgf("Key: %q Cid: %s", k, v)
								return nil
							})

							continue
						}
						if op.Cid == nil {
							log.Warn().Msgf("op.Cid is missing")
						} else if lexutil.LexLink(rcid) != *op.Cid {
							log.Info().Err(fmt.Errorf("mismatch in record op and cid: %s != %s", rcid, *op.Cid))
						}

						if hook.Predicate == nil || hook.Predicate(ctx, commit, op, rec) {
							hook.Action(ctx, commit, op, rec)
						}
					}
				}()
			case e.CursorReset:
				if hook.OnCursorReset == nil {
					continue
				}
				wg.Wait() // Ensure that all in-flight calls are finished.
				hook.OnCursorReset(ctx)
			}
		}
	}
}

func (f *Firehose) closeIfIdle(ctx context.Context, conn *websocket.Conn) chan struct{} {
	log := zerolog.Ctx(ctx)
	ch := make(chan struct{}, 1)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		heartbeat := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				heartbeat = time.Now()
			case <-ticker.C:
				if time.Since(heartbeat) > 5*time.Minute {
					log.Error().Msgf("firehose is idling for too long, disconnecting")
					conn.Close()
					return
				}
			}
		}
	}()

	return ch
}
