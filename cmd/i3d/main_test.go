package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	i3dv1 "github.com/pengubco/i3d/api/i3d/v1"
	"github.com/pengubco/i3d/internal/etcdtest"
	"github.com/pengubco/i3d/internal/snowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ec "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().String()
}

func TestVersion(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, run(t.Context(), []string{"-version"}, &out))
	assert.Contains(t, out.String(), "i3d dev")
}

func TestRequiresConfig(t *testing.T) {
	assert.Error(t, run(t.Context(), nil, &bytes.Buffer{}))
}

func TestEndToEnd(t *testing.T) {
	etcd := etcdtest.Start(t)
	grpcAddr, httpAddr := freeAddr(t), freeAddr(t)
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(cfgPath, fmt.Appendf(nil, `{
		"grpc_addr": %q, "http_addr": %q, "max_workers": 4, "log_level": "warn",
		"etcd": {"endpoints": [%q], "prefix": "/e2e/"}
	}`, grpcAddr, httpAddr, etcd.Endpoint), 0o600))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"-config", cfgPath}, &bytes.Buffer{}) }()

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	health := healthpb.NewHealthClient(conn)
	require.Eventually(t, func() bool {
		resp, err := health.Check(ctx, &healthpb.HealthCheckRequest{Service: "i3d.v1.IDService"})
		return err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING
	}, 20*time.Second, 50*time.Millisecond)

	client := i3dv1.NewIDServiceClient(conn)
	one, err := client.NextID(ctx, &i3dv1.NextIDRequest{})
	require.NoError(t, err)
	batch, err := client.NextIDs(ctx, &i3dv1.NextIDsRequest{Count: 10})
	require.NoError(t, err)
	require.Len(t, batch.GetIds(), 10)
	assert.Greater(t, batch.GetIds()[0], one.GetId())
	ts, _, _ := snowflake.Decompose(one.GetId())
	assert.InDelta(t, time.Now().UnixMilli(), ts, 5000)

	for path, want := range map[string]int{"/readyz": 200, "/livez": 200, "/metrics": 200, "/debug/pprof/": 404} {
		resp, err := http.Get("http://" + httpAddr + path) //nolint:noctx // test
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, want, resp.StatusCode, path)
	}

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("run did not return after cancel")
	}

	// The worker ID was released on shutdown.
	resp, err := etcd.Client(t).Get(t.Context(), "/e2e/workers/live/", ec.WithPrefix(), ec.WithCountOnly())
	require.NoError(t, err)
	assert.Zero(t, resp.Count)
}
