package firehose

import (
	"context"
	"strings"

	"bsky.watch/utils/didset"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func AllOf(predicates ...Predicate) Predicate {
	return func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler) bool {
		for _, p := range predicates {
			if !p(ctx, commit, op, record) {
				return false
			}
		}
		return true
	}
}

func AnyOf(predicates ...Predicate) Predicate {
	return func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler) bool {
		for _, p := range predicates {
			if p(ctx, commit, op, record) {
				return true
			}
		}
		return false
	}
}

func Not(predicate Predicate) Predicate {
	return func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler) bool {
		return !predicate(ctx, commit, op, record)
	}
}

func MentionsDID(did string) Predicate {
	return func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler) bool {
		rec, ok := record.(*bsky.FeedPost)
		if !ok {
			return false
		}
		for _, facet := range rec.Facets {
			for _, feature := range facet.Features {
				if mention := feature.RichtextFacet_Mention; mention != nil {
					if mention.Did == did {
						return true
					}
				}
			}
		}
		return false
	}
}

func CreateOrUpdateOp() Predicate {
	return func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler) bool {
		return op.Action == "create" || op.Action == "update"
	}
}

func DeleteOp() Predicate {
	return func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler) bool {
		return op.Action == "delete"
	}
}

func From(did string) Predicate {
	return func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler) bool {
		return commit.Repo == did
	}
}

func IsInCollection(collection string) Predicate {
	return func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler) bool {
		return strings.HasPrefix(op.Path, collection+"/")
	}

}

func IsBlock() Predicate {
	return IsInCollection("app.bsky.graph.block")
}

func IsPost() Predicate {
	return IsInCollection("app.bsky.feed.post")
}

func IsFollow() Predicate {
	return IsInCollection("app.bsky.graph.follow")
}

func SenderInSet(set didset.QueryableDIDSet) Predicate {
	return func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler) bool {
		r, err := set.Contains(ctx, commit.Repo)
		if err != nil {
			return false
		}
		return r
	}
}

func SenderNotInSet(set didset.QueryableDIDSet) Predicate {
	return func(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit, op *comatproto.SyncSubscribeRepos_RepoOp, record cbg.CBORMarshaler) bool {
		r, err := set.Contains(ctx, commit.Repo)
		if err != nil {
			return false
		}
		return !r
	}
}
