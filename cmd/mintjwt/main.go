// mintjwt issues a short-lived HS256 JWT for the wildlife sightings service.
//
// Example:
//
//	AUTH_JWT_SECRET=devsecret go run ./cmd/mintjwt -sub rama -exp 1h
//
// The secret can come from -secret or from $AUTH_JWT_SECRET. -sub and -exp
// default to "dev" and "1h" so a bare `go run ./cmd/mintjwt` produces a usable
// token.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/YemetsValen/go-kafka-proj/internal/auth"
)

func main() {
	var (
		secret = flag.String("secret", os.Getenv("AUTH_JWT_SECRET"), "HS256 secret (defaults to $AUTH_JWT_SECRET)")
		sub    = flag.String("sub", "dev", "subject claim")
		ttl    = flag.Duration("exp", time.Hour, "token TTL (e.g. 15m, 24h)")
	)
	flag.Parse()

	if *secret == "" {
		fmt.Fprintln(os.Stderr, "error: secret is required (-secret or $AUTH_JWT_SECRET)")
		os.Exit(2)
	}
	tok, err := auth.MintHS256(*secret, *sub, *ttl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(tok)
}
