package server

import (
	"context"
	"errors"
	"net"
	"testing"

	etcdflakev1 "github.com/pengubco/etcdflake/api/etcdflake/v1"
	"github.com/pengubco/etcdflake/internal/snowflake"
	"github.com/pengubco/etcdflake/internal/worker"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeSource struct {
	next int64
	err  error
}

func (f *fakeSource) NextID() (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	f.next++
	return f.next, nil
}

func (f *fakeSource) NextIDs(n int) ([]int64, error) {
	if f.err != nil {
		return nil, f.err
	}
	ids := make([]int64, n)
	for i := range ids {
		f.next++
		ids[i] = f.next
	}
	return ids, nil
}

func dial(t *testing.T, src IDSource) (etcdflakev1.IDServiceClient, *Server) {
	t.Helper()
	srv, err := New(src, prometheus.NewRegistry())
	require.NoError(t, err)

	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	etcdflakev1.RegisterIDServiceServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return etcdflakev1.NewIDServiceClient(conn), srv
}

func TestNextID(t *testing.T) {
	c, srv := dial(t, &fakeSource{})
	resp, err := c.NextID(t.Context(), &etcdflakev1.NextIDRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), resp.GetId())
	assert.InDelta(t, 1, testutil.ToFloat64(srv.issued), 0)
}

func TestNextIDs(t *testing.T) {
	c, srv := dial(t, &fakeSource{})
	resp, err := c.NextIDs(t.Context(), &etcdflakev1.NextIDsRequest{Count: 3})
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3}, resp.GetIds())
	assert.InDelta(t, 3, testutil.ToFloat64(srv.issued), 0)

	_, err = c.NextIDs(t.Context(), &etcdflakev1.NextIDsRequest{Count: MaxBatch})
	require.NoError(t, err)

	for _, n := range []uint32{0, MaxBatch + 1} {
		_, err := c.NextIDs(t.Context(), &etcdflakev1.NextIDsRequest{Count: n})
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "count %d", n)
	}
}

func TestErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		err    error
		code   codes.Code
		reason string
	}{
		{worker.ErrNotReady, codes.Unavailable, "not_ready"},
		{snowflake.ErrStopped, codes.Unavailable, "not_ready"},
		{snowflake.ErrReservationExhausted, codes.Unavailable, "reservation_exhausted"},
		{&snowflake.ClockSkewError{AheadMillis: 2000}, codes.Unavailable, "clock_skew"},
		{snowflake.ErrTimestampOutOfRange, codes.Unavailable, "timestamp_out_of_range"},
		{errors.New("boom"), codes.Internal, "internal"},
	} {
		c, srv := dial(t, &fakeSource{err: tc.err})
		_, err := c.NextID(t.Context(), &etcdflakev1.NextIDRequest{})
		assert.Equal(t, tc.code, status.Code(err), "%v", tc.err)
		_, err = c.NextIDs(t.Context(), &etcdflakev1.NextIDsRequest{Count: 2})
		assert.Equal(t, tc.code, status.Code(err), "%v", tc.err)
		assert.InDelta(t, 2, testutil.ToFloat64(srv.errs.WithLabelValues(tc.reason)), 0, "%v", tc.err)
	}
}
