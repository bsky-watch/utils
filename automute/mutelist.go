package automute

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/rs/zerolog"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"

	"bsky.watch/utils/didset"
)

type List struct {
	url    url.URL
	client *xrpc.Client

	CheckResultExpiration time.Duration
	ListRefreshInterval   time.Duration
	Callback              func(ctx context.Context, client *xrpc.Client, did string) (bool, error)

	mu                 sync.Mutex
	existingEntries    map[string]bool
	negativeCheckCache map[string]time.Time

	checkQueue chan string
}

func New(url *url.URL, authclient *xrpc.Client) *List {
	return &List{
		url:                   *url,
		client:                authclient,
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
	for {
		// TODO: prune negativeCheckCache
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-refresh.C:
			err := l.refreshList(ctx)
			if err != nil {
				log.Error().Err(err).Msgf("Failed to refresh the list %q", l.url.String())
			}
		case did := <-l.checkQueue:
			skip := func(did string) bool {
				l.mu.Lock()
				defer l.mu.Unlock()
				if l.existingEntries[did] {
					return true
				}
				if time.Since(l.negativeCheckCache[did]) < l.CheckResultExpiration {
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
					log.Error().Err(err).Msgf("Failed to check if a user should be added to the list")
					return
				}

				if add {
					err := l.addToList(ctx, did)
					if err != nil {
						log.Error().Err(err).Msgf("Failed to add %q to the list %s", did, l.url.String())
						return
					}
					log.Debug().Msgf("Added %q to the list %s", did, l.url.String())
				}

				l.mu.Lock()
				if add {
					l.existingEntries[did] = true
				} else {
					l.negativeCheckCache[did] = time.Now()
				}
				l.mu.Unlock()
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
	entries, err := didset.MuteList(l.client, l.url.String()).GetDIDs(ctx)
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
