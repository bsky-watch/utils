package didset

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
)

type union struct {
	sets []DIDSet
}

func (u *union) GetDIDs(ctx context.Context) (StringSet, error) {
	log := zerolog.Ctx(ctx).With().
		Str("module", "didset").
		Str("didset", "union").
		Logger()
	ctx = log.WithContext(ctx)

	r := StringSet{}

	for i, s := range u.sets {
		entries, err := s.GetDIDs(ctx)
		if err != nil {
			return nil, fmt.Errorf("evaluating %d'th set of union: %w", i, err)
		}
		for did := range entries {
			r[did] = true
		}
	}
	log.Debug().Msgf("Got %d dids", len(r))

	return r, nil
}

func Union(sets ...DIDSet) DIDSet {
	return &union{sets: sets}
}

type difference struct {
	left  DIDSet
	right DIDSet
}

func (d *difference) GetDIDs(ctx context.Context) (StringSet, error) {
	log := zerolog.Ctx(ctx).With().
		Str("module", "didset").
		Str("didset", "difference").
		Logger()
	ctx = log.WithContext(ctx)

	left, err := d.left.GetDIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("evaluating left side of a difference: %w", err)
	}
	right, err := d.right.GetDIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("evaluating right side of a difference: %w", err)
	}

	for did := range right {
		delete(left, did)
	}
	log.Debug().Msgf("Got %d dids", len(left))

	return left, nil
}

func Difference(left DIDSet, right DIDSet) DIDSet {
	return &difference{left: left, right: right}
}
