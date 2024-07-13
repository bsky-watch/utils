package didset

import (
	"context"
	"fmt"

	"bsky.watch/utils/aturl"
	"bsky.watch/utils/pagination"
	"github.com/rs/zerolog"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"
)

type blocked struct {
	client *xrpc.Client
}

func (b *blocked) GetDIDs(ctx context.Context) (StringSet, error) {
	log := zerolog.Ctx(ctx).With().
		Str("module", "didset").
		Str("didset", "blocked").
		Logger()
	ctx = log.WithContext(ctx)

	r := StringSet{}

	resp, err := comatproto.ServerGetSession(ctx, b.client)
	if err != nil {
		return nil, fmt.Errorf("ServerGetSession: %w", err)
	}
	self := resp.Did

	cursor := ""
	for {
		resp, err := comatproto.RepoListRecords(ctx, b.client, "app.bsky.graph.block", cursor, 100, self, false, "", "")
		if err != nil {
			return nil, fmt.Errorf("listing blocked users: %w", err)
		}
		if resp.Cursor == nil || *resp.Cursor == "" {
			break
		}
		cursor = *resp.Cursor
		for _, rec := range resp.Records {
			item, ok := rec.Value.Val.(*bsky.GraphBlock)
			if !ok {
				continue
			}

			r[item.Subject] = true
		}
	}

	log.Trace().Msgf("Got %d dids", len(r))
	return r, nil
}

func BlockedUsers(authclient *xrpc.Client) DIDSet {
	return &blocked{client: authclient}
}

type blockedBy struct {
	did    string
	client *xrpc.Client
}

func (b *blockedBy) GetDIDs(ctx context.Context) (StringSet, error) {
	log := zerolog.Ctx(ctx).With().
		Str("module", "didset").
		Str("didset", "blocked").
		Logger()
	ctx = log.WithContext(ctx)

	r := StringSet{}

	cursor := ""
	for {
		resp, err := comatproto.RepoListRecords(ctx, b.client, "app.bsky.graph.block", cursor, 100, b.did, false, "", "")
		if err != nil {
			return nil, fmt.Errorf("listing blocked users: %w", err)
		}
		if resp.Cursor == nil || *resp.Cursor == "" {
			break
		}
		cursor = *resp.Cursor
		for _, rec := range resp.Records {
			item, ok := rec.Value.Val.(*bsky.GraphBlock)
			if !ok {
				continue
			}

			r[item.Subject] = true
		}
	}

	log.Trace().Msgf("Got %d dids", len(r))
	return r, nil
}

func BlockedBy(authclient *xrpc.Client, did string) DIDSet {
	return &blockedBy{client: authclient, did: did}
}

type muteList struct {
	client *xrpc.Client
	url    string
}

func (l *muteList) GetDIDs(ctx context.Context) (StringSet, error) {
	log := zerolog.Ctx(ctx).With().
		Str("module", "didset").
		Str("didset", "mutelist").
		Str("list_url", l.url).
		Logger()
	ctx = log.WithContext(ctx)

	u, err := aturl.Parse(l.url)
	if err != nil {
		return nil, err
	}
	did := u.Host

	r, err := pagination.Reduce(
		func(cursor string) (resp *comatproto.RepoListRecords_Output, nextCursor string, err error) {
			resp, err = comatproto.RepoListRecords(ctx, l.client, "app.bsky.graph.listitem", cursor, 100, did, false, "", "")
			if err != nil {
				return
			}
			if resp.Cursor != nil {
				nextCursor = *resp.Cursor
			}
			return
		},
		func(resp *comatproto.RepoListRecords_Output, acc StringSet) (StringSet, error) {
			if acc == nil {
				acc = make(StringSet)
			}
			for _, record := range resp.Records {
				switch r := record.Value.Val.(type) {
				case *bsky.GraphListitem:
					if r.List == l.url {
						acc[r.Subject] = true
					}
				}
			}
			return acc, nil
		},
	)
	if err != nil {
		return nil, err
	}
	log.Trace().Msgf("Got %d dids", len(r))

	return r, nil
}

func MuteList(authclient *xrpc.Client, url string) DIDSet {
	return &muteList{client: authclient, url: url}
}

type followers struct {
	client *xrpc.Client
	did    string
}

func (f *followers) GetDIDs(ctx context.Context) (StringSet, error) {
	log := zerolog.Ctx(ctx).With().
		Str("module", "didset").
		Str("didset", "followers").
		Str("followers_of", f.did).
		Logger()
	ctx = log.WithContext(ctx)

	r := StringSet{}
	cursor := ""
	for {
		resp, err := bsky.GraphGetFollowers(ctx, f.client, f.did, cursor, 100)
		if err != nil {
			return nil, fmt.Errorf("app.bsky.graph.getFollowers: %w", err)
		}

		if len(resp.Followers) == 0 {
			break
		}

		for _, item := range resp.Followers {
			r[item.Did] = true
		}

		if resp.Cursor == nil {
			break
		}
		cursor = *resp.Cursor
	}
	log.Trace().Msgf("Got %d dids", len(r))

	return r, nil
}

func FollowersOf(authclient *xrpc.Client, did string) DIDSet {
	return &followers{client: authclient, did: did}
}

type follows struct {
	client *xrpc.Client
	did    string
}

func (f *follows) GetDIDs(ctx context.Context) (StringSet, error) {
	log := zerolog.Ctx(ctx).With().
		Str("module", "didset").
		Str("didset", "follows").
		Str("follows_of", f.did).
		Logger()
	ctx = log.WithContext(ctx)

	return pagination.Reduce(
		func(cursor string) (resp *bsky.GraphGetFollows_Output, nextCursor string, err error) {
			resp, err = bsky.GraphGetFollows(ctx, f.client, f.did, cursor, 100)
			if err != nil {
				return
			}
			if resp.Cursor != nil {
				nextCursor = *resp.Cursor
			}
			return
		},
		func(resp *bsky.GraphGetFollows_Output, acc StringSet) (StringSet, error) {
			if acc == nil {
				acc = make(StringSet)
			}
			for _, i := range resp.Follows {
				acc[i.Did] = true
			}
			return acc, nil
		},
	)
}

func FollowsOf(authclient *xrpc.Client, did string) DIDSet {
	return &follows{
		client: authclient,
		did:    did,
	}
}

type followRecords struct {
	client *xrpc.Client
	did    string
}

func (f *followRecords) GetDIDs(ctx context.Context) (StringSet, error) {
	log := zerolog.Ctx(ctx).With().
		Str("module", "didset").
		Str("didset", "follows").
		Str("follow_records_of", f.did).
		Logger()
	ctx = log.WithContext(ctx)

	return pagination.Reduce(
		func(cursor string) (resp *comatproto.RepoListRecords_Output, nextCursor string, err error) {
			resp, err = comatproto.RepoListRecords(ctx, f.client, "app.bsky.graph.follow", cursor, 100, f.did, false, "", "")
			if err != nil {
				return
			}
			if resp.Cursor != nil {
				nextCursor = *resp.Cursor
			}
			return
		},
		func(resp *comatproto.RepoListRecords_Output, acc StringSet) (StringSet, error) {
			if acc == nil {
				acc = make(StringSet)
			}
			for _, rec := range resp.Records {
				item, ok := rec.Value.Val.(*bsky.GraphFollow)
				if !ok {
					continue
				}

				acc[item.Subject] = true
			}
			return acc, nil
		},
	)
}

func FollowRecordsOf(authclient *xrpc.Client, did string) DIDSet {
	return &followRecords{
		client: authclient,
		did:    did,
	}
}
