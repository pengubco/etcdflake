// Package etcdtest runs an embedded single-node etcd server for tests.
package etcdtest

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	ec "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

// Server is an embedded etcd server that listens on loopback only.
type Server struct {
	Endpoint string
	etcd     *embed.Etcd
	stop     sync.Once
}

// Start starts an etcd server and stops it when the test ends.
func Start(t testing.TB) *Server {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Logger = "zap"
	cfg.LogLevel = "fatal"
	client, peer := freeURL(t), freeURL(t)
	cfg.ListenClientUrls = []url.URL{client}
	cfg.AdvertiseClientUrls = []url.URL{client}
	cfg.ListenPeerUrls = []url.URL{peer}
	cfg.AdvertisePeerUrls = []url.URL{peer}
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, peer.String())

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("start etcd: %v", err)
	}
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		e.Close()
		t.Fatal("etcd did not become ready")
	}
	s := &Server{Endpoint: client.Host, etcd: e}
	t.Cleanup(s.Stop)
	return s
}

// Stop stops the server. It is safe to call more than once.
func (s *Server) Stop() {
	s.stop.Do(s.etcd.Close)
}

// Client returns a client connected to the server, closed when the test ends.
func (s *Server) Client(t testing.TB) *ec.Client {
	t.Helper()
	cli, err := ec.New(ec.Config{
		Endpoints:   []string{s.Endpoint},
		DialTimeout: 5 * time.Second,
		Logger:      zap.NewNop(),
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func freeURL(t testing.TB) url.URL {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick free port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return url.URL{Scheme: "http", Host: addr}
}
