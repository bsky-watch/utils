package bskyurl

import (
	"fmt"
	"strings"

	"bsky.watch/utils/aturl"
)

type Target interface{}
type TargetWithProfile interface {
	GetProfile() string
}
type TargetRecord interface {
	TargetWithProfile
	GetCollection() string
	GetRKey() string
}

// DetermineTarget parses a string as a Bluesky-related URL. It recognizes the following inputs:
//
//   - Web URL: "https://bsky.app/..."
//   - ATproto URL: "at://..."
//   - Bare DID: "did:..."
func DetermineTarget(s string) (Target, error) {
	if strings.HasPrefix(s, "did:") {
		return &Profile{Profile: s}, nil
	}

	u, err := aturl.Parse(s)
	if err != nil {
		return nil, err
	}

	pathParts := strings.Split(strings.Trim(u.Path, "/"), "/")
	switch u.Scheme {
	case "at":
		switch {
		case len(pathParts) == 0:
			return &Profile{Profile: u.Host}, nil
		case len(pathParts) == 2:
			switch pathParts[0] {
			case "app.bsky.feed.post":
				return &Post{Profile: u.Host, Rkey: pathParts[1]}, nil
			default:
				return &SomeRecord{Profile: u.Host, Collection: pathParts[0], Rkey: pathParts[1]}, nil
			}
		}
	case "http", "https":
		if u.Host != "bsky.app" && u.Host != "staging.bsky.app" {
			return nil, fmt.Errorf("unrecognized hostname: %q", u.Host)
		}

		if len(pathParts) < 2 {
			break
		}
		if pathParts[0] != "profile" {
			break
		}

		switch {
		case len(pathParts) == 2:
			return &Profile{Profile: pathParts[1]}, nil
		case len(pathParts) == 4:
			switch pathParts[2] {
			case "post":
				return &Post{Profile: pathParts[1], Rkey: pathParts[3]}, nil
			}
		}
	}

	return nil, fmt.Errorf("unrecognized URL: %q", s)
}

type Profile struct {
	Profile string
}

func (p *Profile) GetProfile() string { return p.Profile }

type Post struct {
	Profile string
	Rkey    string
}

func (p *Post) GetProfile() string    { return p.Profile }
func (p *Post) GetCollection() string { return "app.bsky.feed.post" }
func (p *Post) GetRKey() string       { return p.Rkey }

// Ensure that it implements the expected interfaces
var _ TargetWithProfile = &Post{}
var _ TargetRecord = &Post{}

type SomeRecord struct {
	Profile    string
	Collection string
	Rkey       string
}

func (r *SomeRecord) GetProfile() string    { return r.Profile }
func (r *SomeRecord) GetCollection() string { return r.Collection }
func (r *SomeRecord) GetRKey() string       { return r.Rkey }

// Ensure that it implements the expected interfaces
var _ TargetWithProfile = &SomeRecord{}
var _ TargetRecord = &SomeRecord{}
