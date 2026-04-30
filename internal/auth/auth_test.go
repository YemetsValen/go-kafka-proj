package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestVerifier_Disabled(t *testing.T) {
	v := New(Config{})
	if v.Enabled() {
		t.Fatalf("expected verifier disabled with empty config")
	}
	p, err := v.Verify("")
	if err != nil || p.Subject != "" {
		t.Errorf("disabled Verify: got p=%+v err=%v", p, err)
	}
}

func TestVerifier_APIKey(t *testing.T) {
	v := New(Config{APIKeys: []string{"alpha-key", "beta"}})
	p, err := v.Verify("alpha-key")
	if err != nil || p.Method != "apikey" {
		t.Fatalf("apikey: %+v err=%v", p, err)
	}
	if _, err := v.Verify("nope"); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("unknown key: want ErrInvalidToken, got %v", err)
	}
	if _, err := v.Verify(""); !errors.Is(err, ErrMissingToken) {
		t.Errorf("empty: want ErrMissingToken, got %v", err)
	}
}

func TestVerifier_JWT(t *testing.T) {
	v := New(Config{JWTSecret: "s3cr3t"})
	tok, err := MintHS256("s3cr3t", "rama", time.Hour)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	p, err := v.Verify(tok)
	if err != nil || p.Subject != "rama" || p.Method != "jwt" {
		t.Errorf("verify: %+v err=%v", p, err)
	}

	// expired token
	expired, _ := MintHS256("s3cr3t", "rama", -time.Minute)
	if _, err := v.Verify(expired); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("expired: want ErrInvalidToken, got %v", err)
	}

	// wrong signing secret
	other, _ := MintHS256("different", "rama", time.Hour)
	if _, err := v.Verify(other); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("wrong secret: want ErrInvalidToken, got %v", err)
	}
}

func TestHTTPMiddleware(t *testing.T) {
	v := New(Config{JWTSecret: "s3cr3t", APIKeys: []string{"svc-key"}})
	tok, _ := MintHS256("s3cr3t", "rama", time.Hour)

	mw := v.HTTPMiddleware()
	final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := FromContext(r.Context())
		if !ok {
			t.Errorf("expected principal in context")
		}
		w.WriteHeader(http.StatusNoContent)
		_, _ = w.Write([]byte(p.Subject))
	})
	h := mw(final)

	cases := []struct {
		name   string
		header string
		want   int
	}{
		{"jwt", "Bearer " + tok, http.StatusNoContent},
		{"apikey", "Bearer svc-key", http.StatusNoContent},
		{"missing", "", http.StatusUnauthorized},
		{"bogus", "Bearer wrong", http.StatusUnauthorized},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/", nil)
			if c.header != "" {
				req.Header.Set("Authorization", c.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != c.want {
				t.Errorf("status=%d want=%d body=%s", rec.Code, c.want, rec.Body.String())
			}
		})
	}
}

func TestHTTPMiddleware_DisabledNoAuth(t *testing.T) {
	v := New(Config{})
	mw := v.HTTPMiddleware()
	called := false
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/", nil))
	if !called || rec.Code != http.StatusNoContent {
		t.Errorf("expected pass-through; called=%v code=%d", called, rec.Code)
	}
}

func TestUnaryInterceptor(t *testing.T) {
	v := New(Config{APIKeys: []string{"svc"}})
	intc := v.UnaryInterceptor()

	mutating := &grpc.UnaryServerInfo{FullMethod: "/sightings.v1.SightingService/CreateSighting"}
	read := &grpc.UnaryServerInfo{FullMethod: "/sightings.v1.SightingService/GetSighting"}
	handler := func(ctx context.Context, _ interface{}) (interface{}, error) { return "ok", nil }

	// mutating without token: rejected
	if _, err := intc(context.Background(), nil, mutating, handler); status.Code(err) != codes.Unauthenticated {
		t.Errorf("missing: want Unauthenticated, got %v", err)
	}
	// mutating with token: passes
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer svc"))
	if _, err := intc(ctx, nil, mutating, handler); err != nil {
		t.Errorf("authorized: %v", err)
	}
	// read RPC: token not required even if missing
	if _, err := intc(context.Background(), nil, read, handler); err != nil {
		t.Errorf("read RPC: %v", err)
	}
}

// fakeServerStream lets us drive StreamInterceptor in unit tests.
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

func TestStreamInterceptor(t *testing.T) {
	v := New(Config{APIKeys: []string{"svc"}})
	intc := v.StreamInterceptor()
	mutating := &grpc.StreamServerInfo{FullMethod: "/sightings.v1.SightingService/Chat"}
	handler := func(_ interface{}, _ grpc.ServerStream) error { return nil }

	if err := intc(nil, &fakeServerStream{ctx: context.Background()}, mutating, handler); status.Code(err) != codes.Unauthenticated {
		t.Errorf("missing: %v", err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer svc"))
	if err := intc(nil, &fakeServerStream{ctx: ctx}, mutating, handler); err != nil {
		t.Errorf("authorized: %v", err)
	}
}

func TestBearerFromHeader(t *testing.T) {
	cases := map[string]string{
		"Bearer abc":    "abc",
		"bearer abc":    "abc",
		"BEARER  abc  ": "abc",
		"":              "",
		"Basic abc":     "",
	}
	for in, want := range cases {
		if got := BearerFromHeader(in); got != want {
			t.Errorf("BearerFromHeader(%q) = %q, want %q", in, got, want)
		}
	}
}
