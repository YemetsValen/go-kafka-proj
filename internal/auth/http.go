package auth

import (
	"encoding/json"
	"errors"
	"net/http"
)

// HTTPMiddleware returns a middleware that validates the Authorization header
// and writes the resulting Principal into the request context.
//
// When the verifier is disabled the middleware is a no-op so dev/test setups
// without secrets still work.
func (v *Verifier) HTTPMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !v.enabled {
				next.ServeHTTP(w, r)
				return
			}
			tok := BearerFromHeader(r.Header.Get("Authorization"))
			p, err := v.Verify(tok)
			if err != nil {
				writeAuthError(w, err)
				return
			}
			next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), p)))
		})
	}
}

func writeAuthError(w http.ResponseWriter, err error) {
	status := http.StatusUnauthorized
	msg := "unauthorized"
	if errors.Is(err, ErrMissingToken) {
		msg = "missing bearer token"
	} else if errors.Is(err, ErrInvalidToken) {
		msg = "invalid bearer token"
	}
	w.Header().Set("WWW-Authenticate", `Bearer realm="sightings"`)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
