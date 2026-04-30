package auth

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MutatingMethods is the set of fully-qualified gRPC method names that require
// authentication. Read-only methods (Get, List, Watch) stay public.
var MutatingMethods = map[string]struct{}{
	"/sightings.v1.SightingService/CreateSighting": {},
	"/sightings.v1.SightingService/UpdateSighting": {},
	"/sightings.v1.SightingService/DeleteSighting": {},
	"/sightings.v1.SightingService/VerifySighting": {},
	"/sightings.v1.SightingService/AddNote":        {},
	"/sightings.v1.SightingService/BulkCreate":     {},
	"/sightings.v1.SightingService/Chat":           {},
}

// IsMutating reports whether the given fully-qualified method requires auth.
func IsMutating(fullMethod string) bool {
	_, ok := MutatingMethods[fullMethod]
	return ok
}

// UnaryInterceptor enforces auth on mutating unary RPCs.
func (v *Verifier) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !v.enabled || !IsMutating(info.FullMethod) {
			return handler(ctx, req)
		}
		ctx, err := v.checkContext(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamInterceptor enforces auth on mutating streaming RPCs.
func (v *Verifier) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !v.enabled || !IsMutating(info.FullMethod) {
			return handler(srv, ss)
		}
		ctx, err := v.checkContext(ss.Context())
		if err != nil {
			return err
		}
		return handler(srv, &serverStreamWithCtx{ServerStream: ss, ctx: ctx})
	}
}

func (v *Verifier) checkContext(ctx context.Context) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	var raw string
	if vals := md.Get("authorization"); len(vals) > 0 {
		raw = vals[0]
	}
	tok := BearerFromHeader(raw)
	if tok == "" {
		// Allow callers to send the bare token under a custom header too,
		// but the documented contract is "authorization: bearer <token>".
		tok = strings.TrimSpace(raw)
	}
	p, err := v.Verify(tok)
	if err != nil {
		return ctx, status.Error(codes.Unauthenticated, err.Error())
	}
	return WithPrincipal(ctx, p), nil
}

// serverStreamWithCtx overrides Context() on a gRPC server stream so the
// principal stored by the interceptor reaches the handler.
type serverStreamWithCtx struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *serverStreamWithCtx) Context() context.Context { return s.ctx }
