package xrpcauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"
)

func jwtExpirationTime(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, fmt.Errorf("expected 3 parts, got %d", len(parts))
	}
	var data struct {
		Expiry float64 `json:"exp"`
	}
	if err := json.NewDecoder(base64.NewDecoder(base64.RawURLEncoding, strings.NewReader(parts[1]))).Decode(&data); err != nil {
		return time.Time{}, fmt.Errorf("failed to decode claim: %w", err)
	}
	if data.Expiry == 0 {
		return time.Time{}, fmt.Errorf("\"exp\" field missing from the claim")
	}
	return time.Unix(int64(data.Expiry), 0), nil
}

type fileBackedTokenSource struct {
	filename string

	refreshMu sync.Mutex
}

func SessionFile(filename string) oauth2.TokenSource {
	return &fileBackedTokenSource{filename: filename}
}

func (s *fileBackedTokenSource) Token() (*oauth2.Token, error) {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()

	b, err := os.ReadFile(s.filename)
	if err != nil {
		return nil, fmt.Errorf("reading token file %q: %w", s.filename, err)
	}
	tok := &comatproto.ServerRefreshSession_Output{}
	if err := json.Unmarshal(b, tok); err != nil {
		return nil, fmt.Errorf("unmarshaling stored token: %w", err)
	}

	expiry, err := jwtExpirationTime(tok.AccessJwt)
	if err != nil {
		return nil, fmt.Errorf("failed to get expiration time from the access token: %w", err)
	}
	if time.Until(expiry) > 5*time.Minute {
		return &oauth2.Token{
			TokenType:    "bearer",
			AccessToken:  tok.AccessJwt,
			RefreshToken: tok.RefreshJwt,
			Expiry:       expiry,
		}, nil
	}

	client := &xrpc.Client{
		Host: "https://bsky.social",
		Auth: &xrpc.AuthInfo{
			AccessJwt: tok.RefreshJwt,
			Handle:    tok.Handle,
			Did:       tok.Did,
		},
	}

	refresh, err := comatproto.ServerRefreshSession(context.Background(), client)
	if err != nil {
		return nil, fmt.Errorf("failed to refresh the token: %w", err)
	}

	b, err = json.Marshal(refresh)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal the refreshed token: %w", err)
	}
	if err := os.WriteFile(s.filename+".tmp", b, 0600); err != nil {
		return nil, fmt.Errorf("failed to write the token into a temp file: %w", err)
	}
	if err := os.Rename(s.filename+".tmp", s.filename); err != nil {
		return nil, fmt.Errorf("failed to replace the token file: %w", err)
	}

	expiry, err = jwtExpirationTime(refresh.AccessJwt)
	if err != nil {
		return nil, fmt.Errorf("failed to get expiration time from the access token: %w", err)
	}

	r := &oauth2.Token{
		TokenType:    "bearer",
		AccessToken:  refresh.AccessJwt,
		RefreshToken: refresh.RefreshJwt,
		Expiry:       expiry,
	}

	return r, nil
}

func NewHttpClient(ctx context.Context, authfile string) *http.Client {
	return oauth2.NewClient(ctx, SessionFile(authfile))
}

func NewClient(ctx context.Context, authfile string) *xrpc.Client {
	return NewClientWithTokenSource(ctx, SessionFile(authfile))
}

func NewAnonymousClient(ctx context.Context) *xrpc.Client {
	return &xrpc.Client{
		Host: "https://bsky.social",
	}
}

func NewClientWithTokenSource(ctx context.Context, source oauth2.TokenSource) *xrpc.Client {
	r := &xrpc.Client{
		Client: oauth2.NewClient(ctx, source),
		Host:   "https://bsky.social",
	}
	tr, ok := r.Client.Transport.(*oauth2.Transport)
	if ok {
		tr.Base = stripAuthHeaderOnRedirect(r)
	}
	return r
}

type passwordAuthTokenSource struct {
	sync.Mutex

	Login    string
	Password string

	Session   comatproto.ServerRefreshSession_Output
	Timestamp time.Time

	Filename string
}

func PasswordAuth(login string, password string) oauth2.TokenSource {
	return &passwordAuthTokenSource{
		Login:    login,
		Password: password,
	}
}

func PasswordAuthWithFileCache(login string, password string, filename string) oauth2.TokenSource {
	return &passwordAuthTokenSource{
		Login:    login,
		Password: password,
		Filename: filename,
	}
}

func (s *passwordAuthTokenSource) Token() (*oauth2.Token, error) {
	s.Lock()
	defer s.Unlock()
	ctx := context.Background()

	switch {
	case s.Timestamp.IsZero():
		// First request, we don't have any token yet.
		if s.Filename != "" {
			err := s.restoreFromFile(s.Filename)
			if err != nil {
				// fall through to creating a new session
			} else {
				client := NewAnonymousClient(ctx)
				client.Auth = &xrpc.AuthInfo{
					AccessJwt: s.Session.AccessJwt,
					Handle:    s.Session.Handle,
					Did:       s.Session.Did,
				}
				_, err := comatproto.ServerGetSession(ctx, client)
				if err != nil {
					// fall through to creating a new session
				} else {
					break // exit from switch statement to return the token we just loaded
				}
			}
		}

		client := NewAnonymousClient(ctx)
		resp, err := comatproto.ServerCreateSession(ctx, client, &comatproto.ServerCreateSession_Input{
			Identifier: s.Login,
			Password:   s.Password,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create a session: %w", err)
		}
		s.Session.AccessJwt = resp.AccessJwt
		s.Session.RefreshJwt = resp.RefreshJwt
		s.Session.Did = resp.Did
		s.Session.Handle = resp.Handle
		s.Timestamp = time.Now()
	case s.Timestamp.Add(time.Minute).Before(time.Now()):
		// We have a token, but it might be stale; get a new one.
		client := NewAnonymousClient(ctx)
		client.Auth = &xrpc.AuthInfo{
			AccessJwt: s.Session.RefreshJwt,
			Handle:    s.Session.Handle,
			Did:       s.Session.Did,
		}
		resp, err := comatproto.ServerRefreshSession(ctx, client)
		if err != nil {
			return nil, fmt.Errorf("failed to refresh the token: %w", err)
		}
		s.Session = *resp
		s.Timestamp = time.Now()
		_ = s.saveToFile(s.Filename)
	}

	expiry, err := jwtExpirationTime(s.Session.AccessJwt)
	if err != nil {
		return nil, fmt.Errorf("failed to get expiration time from the access token: %w", err)
	}

	r := &oauth2.Token{
		TokenType:    "bearer",
		AccessToken:  s.Session.AccessJwt,
		RefreshToken: s.Session.RefreshJwt,
		Expiry:       expiry,
	}

	return r, nil
}

func (s *passwordAuthTokenSource) restoreFromFile(filename string) error {
	b, err := os.ReadFile(filename)
	if err != nil {
		fmt.Errorf("reading token file %q: %w", filename, err)
	}
	tok := &comatproto.ServerRefreshSession_Output{}
	if err := json.Unmarshal(b, tok); err != nil {
		return fmt.Errorf("unmarshaling stored token: %w", err)
	}
	s.Session = *tok
	s.Timestamp = time.Now()
	return nil
}

func (s *passwordAuthTokenSource) saveToFile(filename string) error {
	b, err := json.Marshal(s.Session)
	if err != nil {
		return fmt.Errorf("failed to marshal the refreshed token: %w", err)
	}
	if err := os.WriteFile(filename+".tmp", b, 0600); err != nil {
		return fmt.Errorf("failed to write the token into a temp file: %w", err)
	}
	if err := os.Rename(filename+".tmp", filename); err != nil {
		return fmt.Errorf("failed to replace the token file: %w", err)
	}
	return nil
}
