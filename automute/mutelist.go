package automute

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/imax9000/errors"

	"bsky.watch/utils/listserver"
	"github.com/rs/zerolog"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
)

type TransientError struct {
	Err error
}

func (err *TransientError) Unwrap() error {
	return err.Err
}

func (err *TransientError) Error() string {
	return err.Err.Error()
}

type List struct {
	url        url.URL
	client     *xrpc.Client
	listServer *listserver.Server

	CheckResultExpiration time.Duration
	ListRefreshInterval   time.Duration
	Callback              func(ctx context.Context, client *xrpc.Client, did string) (bool, error)

	mu                 sync.Mutex
	existingEntries    map[string]bool
	negativeCheckCache map[string]time.Time

	checkQueue chan string
}

func New(url *url.URL, authclient *xrpc.Client, listServer *listserver.Server) *List {
	return &List{
		url:                   *url,
		client:                authclient,
		listServer:            listServer,
		existingEntries:       map[string]bool{},
		negativeCheckCache:    map[string]time.Time{},
		CheckResultExpiration: 24 * time.Hour,
		ListRefreshInterval:   30 * time.Minute,
		checkQueue:            make(chan string, 50),
	}
}

func (l *List) Run(ctx context.Context) error {
	log := zerolog.Ctx(ctx).With().
		Str("module", "automute").
		Str("list_did", l.url.String()).
		Logger()
	ctx = log.WithContext(ctx)

	for {
		err := l.refreshList(ctx)
		if err != nil {
			log.Error().Err(err).Msgf("Failed to refresh the list %q", l.url.String())
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}

	refresh := time.NewTicker(l.ListRefreshInterval)
	defer refresh.Stop()

	pruneInterval := l.CheckResultExpiration / 10
	if pruneInterval < time.Minute {
		pruneInterval = time.Minute
	}
	if pruneInterval > 6*time.Hour {
		pruneInterval = 6 * time.Hour
	}
	prune := time.NewTicker(pruneInterval)
	defer prune.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-refresh.C:
			err := l.refreshList(ctx)
			if err != nil {
				log.Error().Err(err).Msgf("Failed to refresh the list %q", l.url.String())
			}
		case <-prune.C:
			start := time.Now()
			cutoff := start.Add(-l.CheckResultExpiration)

			log.Debug().Msgf("Starting cache cleanup...")

			l.mu.Lock()
			deleted := 0
			total := len(l.negativeCheckCache)
			for did, ts := range l.negativeCheckCache {
				if ts.Before(cutoff) {
					delete(l.negativeCheckCache, did)
					deleted++
				}
			}
			l.mu.Unlock()

			duration := time.Since(start)
			log.Info().Dur("duration", duration).Int("deleted", deleted).
				Msgf("Cache cleanup done. It took %s and deleted %d out of %d entries",
					duration, deleted, total)

		case did := <-l.checkQueue:
			skip := func(did string) bool {
				l.mu.Lock()
				defer l.mu.Unlock()
				if l.existingEntries[did] {
					requestCache.WithLabelValues(l.url.String(), "hit", "true").Inc()
					return true
				}
				if time.Since(l.negativeCheckCache[did]) < l.CheckResultExpiration {
					requestCache.WithLabelValues(l.url.String(), "hit", "false").Inc()
					return true
				}
				return false
			}(did)

			if skip {
				break
			}

			func(did string) {
				if l.Callback == nil {
					return
				}
				add, err := l.Callback(ctx, l.client, did)
				if err != nil {
					if _, ok := errors.As[*TransientError](err); !ok {
						log.Error().Err(err).Msgf("Failed to check if a user should be added to the list")
						requestCache.WithLabelValues(l.url.String(), "miss", "callback_error").Inc()
					} else {
						requestCache.WithLabelValues(l.url.String(), "miss", "transient_error").Inc()
					}
					return
				}

				if add {
					err := l.addToList(ctx, did)
					if err != nil {
						log.Error().Err(err).Msgf("Failed to add %q to the list %s", did, l.url.String())
						requestCache.WithLabelValues(l.url.String(), "miss", "update_error").Inc()
						return
					}
					log.Debug().Msgf("Added %q to the list %s", did, l.url.String())
				}

				l.mu.Lock()
				if add {
					l.existingEntries[did] = true
				} else {
					l.negativeCheckCache[did] = time.Now()
					negativeCacheSize.WithLabelValues(l.url.String()).Set(float64(len(l.negativeCheckCache)))
				}
				l.mu.Unlock()

				requestCache.WithLabelValues(l.url.String(), "miss", fmt.Sprintf("%t", add)).Inc()
			}(did)
		}
	}
	return ctx.Err()
}

func (l *List) addToList(ctx context.Context, did string) error {
	_, err := comatproto.RepoCreateRecord(ctx, l.client, &comatproto.RepoCreateRecord_Input{
		Collection: "app.bsky.graph.listitem",
		Repo:       l.url.Host,
		Record: &lexutil.LexiconTypeDecoder{Val: &bsky.GraphListitem{
			List:      l.url.String(),
			Subject:   did,
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
		}},
	})

	return err
}

func (l *List) refreshList(ctx context.Context) error {
	s, err := l.listServer.List(l.url.String())
	if err != nil {
		return err
	}
	entries, err := s.GetDIDs(ctx)
	if err != nil {
		return err
	}

	l.mu.Lock()
	l.existingEntries = entries
	l.mu.Unlock()
	return nil
}

func (l *List) Check(did string) {
	defer recover()
	l.checkQueue <- did
}
