// Package server implements the etcdflake.v1.IDService gRPC API.
package server

import (
	"context"
	"errors"

	etcdflakev1 "github.com/pengubco/etcdflake/api/etcdflake/v1"
	"github.com/pengubco/etcdflake/internal/snowflake"
	"github.com/pengubco/etcdflake/internal/worker"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MaxBatch is the largest count NextIDs accepts.
const MaxBatch = 4096

// IDSource issues IDs. *worker.Worker implements it.
type IDSource interface {
	NextID() (int64, error)
	NextIDs(n int) ([]int64, error)
}

// Server serves IDs from an IDSource.
type Server struct {
	etcdflakev1.UnimplementedIDServiceServer

	src    IDSource
	issued prometheus.Counter
	errs   *prometheus.CounterVec
}

// New returns a Server. Metrics are registered with reg if it isn't nil.
func New(src IDSource, reg prometheus.Registerer) (*Server, error) {
	s := &Server{
		src: src,
		issued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "etcdflake_ids_issued_total",
			Help: "Number of IDs issued.",
		}),
		errs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "etcdflake_id_errors_total",
			Help: "Number of failed ID requests by reason.",
		}, []string{"reason"}),
	}
	if reg != nil {
		for _, c := range []prometheus.Collector{s.issued, s.errs} {
			if err := reg.Register(c); err != nil {
				return nil, err
			}
		}
	}
	return s, nil
}

// NextID returns one ID.
func (s *Server) NextID(context.Context, *etcdflakev1.NextIDRequest) (*etcdflakev1.NextIDResponse, error) {
	id, err := s.src.NextID()
	if err != nil {
		return nil, s.toStatus(err)
	}
	s.issued.Inc()
	return &etcdflakev1.NextIDResponse{Id: id}, nil
}

// NextIDs returns req.Count increasing IDs.
func (s *Server) NextIDs(_ context.Context, req *etcdflakev1.NextIDsRequest) (*etcdflakev1.NextIDsResponse, error) {
	n := req.GetCount()
	if n < 1 || n > MaxBatch {
		s.errs.WithLabelValues("invalid_argument").Inc()
		return nil, status.Errorf(codes.InvalidArgument, "count %d out of range [1, %d]", n, MaxBatch)
	}
	ids, err := s.src.NextIDs(int(n))
	if err != nil {
		return nil, s.toStatus(err)
	}
	s.issued.Add(float64(len(ids)))
	return &etcdflakev1.NextIDsResponse{Ids: ids}, nil
}

func (s *Server) toStatus(err error) error {
	var skew *snowflake.ClockSkewError
	reason, code, msg := "internal", codes.Internal, "internal error"
	switch {
	case errors.Is(err, worker.ErrNotReady), errors.Is(err, snowflake.ErrStopped):
		reason, code, msg = "not_ready", codes.Unavailable, "node not ready"
	case errors.Is(err, snowflake.ErrReservationExhausted):
		reason, code, msg = "reservation_exhausted", codes.Unavailable, "node cannot reach coordinator"
	case errors.As(err, &skew):
		reason, code, msg = "clock_skew", codes.Unavailable, "clock skew"
	case errors.Is(err, snowflake.ErrTimestampOutOfRange):
		reason, code, msg = "timestamp_out_of_range", codes.Unavailable, "clock out of range"
	}
	s.errs.WithLabelValues(reason).Inc()
	return status.Error(code, msg)
}
