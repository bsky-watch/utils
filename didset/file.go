package didset

import (
	"context"
	"fmt"
	"os"
	"strings"
)

type fromFile string

func FromFile(name string) DIDSet {
	return fromFile(name)
}

func (name fromFile) GetDIDs(ctx context.Context) (StringSet, error) {
	b, err := os.ReadFile(string(name))
	if err != nil {
		return nil, fmt.Errorf("reading file %q: %w", name, err)
	}
	r := StringSet{}
	for _, l := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(l, "did:") {
			continue
		}
		r[strings.TrimSpace(l)] = true
	}
	return r, nil
}
