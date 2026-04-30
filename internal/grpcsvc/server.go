// Package grpcsvc implements the SightingService gRPC server on top of
// store.Store and the Kafka producer.
package grpcsvc

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	pb "github.com/YemetsValen/go-kafka-proj/gen/go/sightings/v1"
	"github.com/YemetsValen/go-kafka-proj/internal/events"
	"github.com/YemetsValen/go-kafka-proj/internal/models"
	"github.com/YemetsValen/go-kafka-proj/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Publisher is the minimal Kafka surface the service needs.
type Publisher interface {
	Publish(ctx context.Context, key, eventType string, payload interface{}) error
}

// Server implements pb.SightingServiceServer.
type Server struct {
	pb.UnimplementedSightingServiceServer

	store    store.Store
	producer Publisher
	bus      *events.Bus
	chat     *chatBroadcast
}

func New(s store.Store, p Publisher, bus *events.Bus) *Server {
	if bus == nil {
		bus = events.New()
	}
	return &Server{
		store:    s,
		producer: p,
		bus:      bus,
		chat:     newChatBroadcast(),
	}
}

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

func (s *Server) CreateSighting(ctx context.Context, req *pb.CreateSightingRequest) (*pb.CreateSightingResponse, error) {
	if strings.TrimSpace(req.GetSpecies()) == "" || strings.TrimSpace(req.GetObservedBy()) == "" {
		return nil, status.Error(codes.InvalidArgument, "species and observed_by are required")
	}
	observedAt := req.GetObservedAt().AsTime()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	in := models.Sighting{
		ID:         uuid.NewString(),
		Species:    req.GetSpecies(),
		Location:   req.GetLocation(),
		Latitude:   req.GetPosition().GetLatitude(),
		Longitude:  req.GetPosition().GetLongitude(),
		ObservedBy: req.GetObservedBy(),
		ObservedAt: observedAt,
	}
	saved, err := s.store.Create(ctx, in)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create: %v", err)
	}
	s.publishAndBroadcast(ctx, "sighting.created", events.TypeCreated, saved, nil)
	return &pb.CreateSightingResponse{Sighting: toProto(saved)}, nil
}

func (s *Server) GetSighting(ctx context.Context, req *pb.GetSightingRequest) (*pb.GetSightingResponse, error) {
	got, err := s.store.Get(ctx, req.GetId())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "sighting not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	return &pb.GetSightingResponse{Sighting: toProto(got)}, nil
}

func (s *Server) ListSightings(ctx context.Context, req *pb.ListSightingsRequest) (*pb.ListSightingsResponse, error) {
	res, err := s.store.List(ctx, store.ListFilter{
		Species:      req.GetSpeciesFilter(),
		OnlyVerified: req.GetOnlyVerified(),
		PageSize:     int(req.GetPageSize()),
		PageToken:    req.GetPageToken(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	out := make([]*pb.Sighting, 0, len(res.Sightings))
	for _, x := range res.Sightings {
		out = append(out, toProto(x))
	}
	//nolint:gosec // pagination total is small enough not to overflow int32 in practice.
	return &pb.ListSightingsResponse{
		Sightings:     out,
		NextPageToken: res.NextPageToken,
		Total:         int32(res.Total),
	}, nil
}

func (s *Server) UpdateSighting(ctx context.Context, req *pb.UpdateSightingRequest) (*pb.UpdateSightingResponse, error) {
	in := req.GetSighting()
	if in == nil || in.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "sighting.id is required")
	}
	maskFields := req.GetUpdateMask().GetPaths()
	hasField := func(name string) bool {
		if len(maskFields) == 0 {
			return true
		}
		for _, f := range maskFields {
			if f == name {
				return true
			}
		}
		return false
	}

	saved, err := s.store.Update(ctx, in.GetId(), func(cur *models.Sighting) {
		if hasField("species") && in.GetSpecies() != "" {
			cur.Species = in.GetSpecies()
		}
		if hasField("location") {
			cur.Location = in.GetLocation()
		}
		if hasField("position") && in.GetPosition() != nil {
			cur.Latitude = in.GetPosition().GetLatitude()
			cur.Longitude = in.GetPosition().GetLongitude()
		}
		if hasField("observed_by") && in.GetObservedBy() != "" {
			cur.ObservedBy = in.GetObservedBy()
		}
		if hasField("observed_at") && in.GetObservedAt() != nil {
			cur.ObservedAt = in.GetObservedAt().AsTime()
		}
		if hasField("verified") {
			cur.Verified = in.GetVerified()
		}
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "sighting not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update: %v", err)
	}
	s.publishAndBroadcast(ctx, "sighting.updated", events.TypeUpdated, saved, nil)
	return &pb.UpdateSightingResponse{Sighting: toProto(saved)}, nil
}

func (s *Server) DeleteSighting(ctx context.Context, req *pb.DeleteSightingRequest) (*pb.DeleteSightingResponse, error) {
	got, err := s.store.Get(ctx, req.GetId())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "sighting not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "lookup: %v", err)
	}
	if err := s.store.Delete(ctx, req.GetId()); err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	s.publishAndBroadcast(ctx, "sighting.deleted", events.TypeDeleted, got, nil)
	return &pb.DeleteSightingResponse{}, nil
}

func (s *Server) VerifySighting(ctx context.Context, req *pb.VerifySightingRequest) (*pb.VerifySightingResponse, error) {
	saved, err := s.store.Update(ctx, req.GetId(), func(cur *models.Sighting) {
		cur.Verified = true
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "sighting not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "verify: %v", err)
	}
	s.publishAndBroadcast(ctx, "sighting.verified", events.TypeVerified, saved, nil)
	return &pb.VerifySightingResponse{Sighting: toProto(saved)}, nil
}

func (s *Server) AddNote(ctx context.Context, req *pb.AddNoteRequest) (*pb.AddNoteResponse, error) {
	if req.GetNote() == nil || strings.TrimSpace(req.GetNote().GetAuthor()) == "" || strings.TrimSpace(req.GetNote().GetText()) == "" {
		return nil, status.Error(codes.InvalidArgument, "note.author and note.text are required")
	}
	note := models.Note{
		Author:    req.GetNote().GetAuthor(),
		Text:      req.GetNote().GetText(),
		CreatedAt: time.Now().UTC(),
	}
	saved, err := s.store.AddNote(ctx, req.GetSightingId(), note)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "sighting not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "add note: %v", err)
	}
	s.publishAndBroadcast(ctx, "sighting.note_added", events.TypeNoteAdded, saved, &note)
	return &pb.AddNoteResponse{Sighting: toProto(saved)}, nil
}

// ---------------------------------------------------------------------------
// Streaming
// ---------------------------------------------------------------------------

// Watch tails a stream of sighting events. Currently fed from in-process
// broadcasts triggered by mutating RPCs; a Kafka consumer can be plugged in
// transparently because the broadcast channel is the integration point.
func (s *Server) Watch(req *pb.WatchRequest, stream grpc.ServerStreamingServer[pb.WatchResponse]) error {
	if req.GetFromBeginning() {
		// Replay current state once before tailing live events.
		res, err := s.store.List(stream.Context(), store.ListFilter{PageSize: 200})
		if err != nil {
			return status.Errorf(codes.Internal, "replay: %v", err)
		}
		for _, x := range res.Sightings {
			if req.GetSightingId() != "" && x.ID != req.GetSightingId() {
				continue
			}
			ev := &pb.WatchResponse{
				Type:      pb.EventType_EVENT_TYPE_CREATED,
				Timestamp: timestamppb.New(x.CreatedAt),
				Sighting:  toProto(x),
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}

	sub := s.bus.Subscribe()
	defer s.bus.Unsubscribe(sub)

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case ev, ok := <-sub:
			if !ok {
				return nil
			}
			if req.GetSightingId() != "" && ev.Sighting.ID != req.GetSightingId() {
				continue
			}
			if err := stream.Send(eventToWatchResponse(ev)); err != nil {
				return err
			}
		}
	}
}

func (s *Server) BulkCreate(stream grpc.ClientStreamingServer[pb.BulkCreateRequest, pb.BulkCreateResponse]) error {
	var (
		received, created, failed int32
		errs                      []*pb.BulkCreateError
		index                     int32
	)
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			//nolint:gosec // counts won't realistically overflow int32 in a single bulk call.
			return stream.SendAndClose(&pb.BulkCreateResponse{
				Received: received,
				Created:  created,
				Failed:   failed,
				Errors:   errs,
			})
		}
		if err != nil {
			return err
		}
		received++
		idx := index
		index++

		one := req.GetSighting()
		if one == nil || strings.TrimSpace(one.GetSpecies()) == "" || strings.TrimSpace(one.GetObservedBy()) == "" {
			failed++
			errs = append(errs, &pb.BulkCreateError{Index: idx, Reason: "species and observed_by are required"})
			continue
		}

		observedAt := one.GetObservedAt().AsTime()
		if observedAt.IsZero() {
			observedAt = time.Now().UTC()
		}
		in := models.Sighting{
			ID:         uuid.NewString(),
			Species:    one.GetSpecies(),
			Location:   one.GetLocation(),
			Latitude:   one.GetPosition().GetLatitude(),
			Longitude:  one.GetPosition().GetLongitude(),
			ObservedBy: one.GetObservedBy(),
			ObservedAt: observedAt,
		}
		saved, err := s.store.Create(stream.Context(), in)
		if err != nil {
			failed++
			errs = append(errs, &pb.BulkCreateError{Index: idx, Reason: err.Error()})
			continue
		}
		created++
		s.publishAndBroadcast(stream.Context(), "sighting.created", events.TypeCreated, saved, nil)
	}
}

func (s *Server) Chat(stream grpc.BidiStreamingServer[pb.ChatRequest, pb.ChatResponse]) error {
	// One Chat call subscribes to the chat broadcast. Each ChatMessage from
	// the client is also published to Kafka as `sighting.chat`.
	sub := s.chat.Subscribe()
	defer s.chat.Unsubscribe(sub)

	var wg sync.WaitGroup
	wg.Add(1)
	errCh := make(chan error, 2)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stream.Context().Done():
				return
			case msg, ok := <-sub:
				if !ok {
					return
				}
				if err := stream.Send(msg); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	for {
		in, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			errCh <- err
			break
		}
		out := &pb.ChatResponse{
			SightingId: in.GetSightingId(),
			Author:     in.GetAuthor(),
			Text:       in.GetText(),
			SentAt:     timestamppb.Now(),
		}
		s.chat.Publish(out)
		_ = s.producer.Publish(stream.Context(), in.GetSightingId(), "sighting.chat", out)
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func (s *Server) publishAndBroadcast(ctx context.Context, kafkaType string, evType events.Type, sighting models.Sighting, note *models.Note) {
	payload := map[string]interface{}{"sighting": sighting}
	if note != nil {
		payload["note"] = *note
	}
	_ = s.producer.Publish(ctx, sighting.ID, kafkaType, payload)

	s.bus.Publish(events.Event{
		Type:      evType,
		Timestamp: time.Now().UTC(),
		Sighting:  sighting,
		Note:      note,
	})
}

func eventToWatchResponse(ev events.Event) *pb.WatchResponse {
	var pbType pb.EventType
	switch ev.Type {
	case events.TypeCreated:
		pbType = pb.EventType_EVENT_TYPE_CREATED
	case events.TypeUpdated:
		pbType = pb.EventType_EVENT_TYPE_UPDATED
	case events.TypeNoteAdded:
		pbType = pb.EventType_EVENT_TYPE_NOTE_ADDED
	case events.TypeVerified:
		pbType = pb.EventType_EVENT_TYPE_VERIFIED
	case events.TypeDeleted:
		pbType = pb.EventType_EVENT_TYPE_DELETED
	default:
		pbType = pb.EventType_EVENT_TYPE_UNSPECIFIED
	}
	out := &pb.WatchResponse{
		Type:      pbType,
		Timestamp: timestamppb.New(ev.Timestamp),
		Sighting:  toProto(ev.Sighting),
	}
	if ev.Note != nil {
		out.Note = &pb.Note{
			Author:    ev.Note.Author,
			Text:      ev.Note.Text,
			CreatedAt: timestamppb.New(ev.Note.CreatedAt),
		}
	}
	return out
}

func toProto(s models.Sighting) *pb.Sighting {
	notes := make([]*pb.Note, 0, len(s.Notes))
	for _, n := range s.Notes {
		notes = append(notes, &pb.Note{
			Author:    n.Author,
			Text:      n.Text,
			CreatedAt: timestamppb.New(n.CreatedAt),
		})
	}
	return &pb.Sighting{
		Id:         s.ID,
		Species:    s.Species,
		Location:   s.Location,
		Position:   &pb.GeoPoint{Latitude: s.Latitude, Longitude: s.Longitude},
		ObservedBy: s.ObservedBy,
		ObservedAt: timestamppb.New(s.ObservedAt),
		Notes:      notes,
		Verified:   s.Verified,
		CreatedAt:  timestamppb.New(s.CreatedAt),
		UpdatedAt:  timestamppb.New(s.UpdatedAt),
	}
}
