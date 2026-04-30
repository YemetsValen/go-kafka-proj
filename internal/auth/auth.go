// Package auth provides bearer-token authentication for HTTP and gRPC.
//
// Two token shapes are supported, both presented as `Authorization: Bearer <t>`:
//
//   - JWT (HS256) signed with the shared secret in AUTH_JWT_SECRET.
//   - Static API keys from the comma-separated AUTH_API_KEYS list.
//
// If neither is configured the verifier is disabled and every request passes;
// a startup warning is logged so the operator notices.
package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Errors returned by Verifier.Verify.
var (
	ErrMissingToken = errors.New("missing bearer token")
	ErrInvalidToken = errors.New("invalid bearer token")
)

// Principal identifies the caller of an authenticated request.
type Principal struct {
	// Subject is the authenticated identity. For JWT it's the `sub` claim; for
	// API-key auth it's "apikey:<key-prefix>" so logs don't leak the full key.
	Subject string
	// Method is "jwt" or "apikey".
	Method string
}

// Verifier validates incoming bearer tokens.
type Verifier struct {
	jwtSecret []byte
	apiKeys   map[string]struct{}
	enabled   bool
}

// Config configures a Verifier.
type Config struct {
	JWTSecret string
	APIKeys   []string
}

// New returns a Verifier for the given config. If both JWTSecret and APIKeys
// are empty the verifier is disabled and Verify always returns a Principal{}
// without error.
func New(cfg Config) *Verifier {
	v := &Verifier{}
	if cfg.JWTSecret != "" {
		v.jwtSecret = []byte(cfg.JWTSecret)
		v.enabled = true
	}
	if len(cfg.APIKeys) > 0 {
		v.apiKeys = make(map[string]struct{}, len(cfg.APIKeys))
		for _, k := range cfg.APIKeys {
			k = strings.TrimSpace(k)
			if k != "" {
				v.apiKeys[k] = struct{}{}
			}
		}
		if len(v.apiKeys) > 0 {
			v.enabled = true
		}
	}
	return v
}

// Enabled reports whether any auth method is configured.
func (v *Verifier) Enabled() bool { return v.enabled }

// Verify validates a token and returns the authenticated Principal.
//
// When the verifier is disabled it returns a zero-value Principal and no
// error so handlers can run unauthenticated in dev.
func (v *Verifier) Verify(token string) (Principal, error) {
	if !v.enabled {
		return Principal{}, nil
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return Principal{}, ErrMissingToken
	}

	// Static API keys take precedence — they're cheap to check.
	if _, ok := v.apiKeys[token]; ok {
		return Principal{Subject: "apikey:" + keyPrefix(token), Method: "apikey"}, nil
	}

	if v.jwtSecret == nil {
		return Principal{}, ErrInvalidToken
	}
	parsed, err := jwt.Parse(token, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return v.jwtSecret, nil
	}, jwt.WithValidMethods([]string{"HS256"}))
	if err != nil || !parsed.Valid {
		return Principal{}, ErrInvalidToken
	}
	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		return Principal{}, ErrInvalidToken
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return Principal{}, ErrInvalidToken
	}
	return Principal{Subject: sub, Method: "jwt"}, nil
}

// MintHS256 issues a short HS256 JWT for the given subject and TTL. Used by
// cmd/mintjwt and tests.
func MintHS256(secret, subject string, ttl time.Duration) (string, error) {
	if secret == "" || subject == "" {
		return "", errors.New("secret and subject are required")
	}
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": subject,
		"iat": now.Unix(),
		"exp": now.Add(ttl).Unix(),
	})
	return tok.SignedString([]byte(secret))
}

func keyPrefix(k string) string {
	if len(k) <= 6 {
		return k
	}
	return k[:6]
}

// ---- context helpers ----

type principalKey struct{}

// WithPrincipal stores p in ctx so downstream handlers can read it.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// FromContext returns the Principal stored in ctx, if any.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// BearerFromHeader extracts the token from a value like "Bearer xyz".
func BearerFromHeader(h string) string {
	const prefix = "Bearer "
	if len(h) < len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}
