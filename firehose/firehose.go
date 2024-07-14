package firehose

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
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

type Firehose struct {
	Hooks []Hook
	Host  string

	seq   int64
	ident string
}

type Predicate func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler) bool

type Hook struct {
	Predicate Predicate
	Action    func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler)
}

func New() *Firehose {
	return &Firehose{ident: "bsky.watch/utils/firehose"}
}

func (f *Firehose) Run(ctx context.Context) error {

	log := zerolog.Ctx(ctx).With().Str("module", "firehose").Logger()
	ctx = log.WithContext(ctx)

	if f.Host == "" {
		f.Host = "bsky.network"
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

		callbacks := &events.RepoStreamCallbacks{
			RepoCommit: func(e *comatproto.SyncSubscribeRepos_Commit) error {
				log := log.With().
					Int64("seq", e.Seq).
					Bool("rebase", e.Rebase).
					Bool("tooBig", e.TooBig).
					Str("commit_time", e.Time).
					Str("repo", e.Repo).
					Str("commit", e.Commit.String()).
					Logger()

				defer func() {
					if err := recover(); err != nil {
						log.Error().Msgf("RepoCommit callback has panicked: %+v", err)
					}
				}()
				f.seq = e.Seq
				repo_, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(e.Blocks))
				if err != nil {
					return fmt.Errorf("ReadRepoFromCar: %w", err)
				}
				for _, op := range e.Ops {
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

					for _, hook := range f.Hooks {
						if hook.Action == nil {
							continue
						}
						if hook.Predicate == nil || hook.Predicate(ctx, e, op, rec) {
							// TODO: serialize action calls to avoid any possible out-of-order invocations
							go hook.Action(ctx, e, op, rec)
						}
					}
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
