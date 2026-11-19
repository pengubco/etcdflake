// Command etcdflake serves unique, increasing 64-bit IDs over gRPC, coordinated through etcd.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	etcdflakev1 "github.com/pengubco/etcdflake/api/etcdflake/v1"
	"github.com/pengubco/etcdflake/internal/config"
	"github.com/pengubco/etcdflake/internal/server"
	"github.com/pengubco/etcdflake/internal/worker"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	ec "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// Set at build time with -ldflags "-X main.version=... -X main.commit=...".
var (
	version = "dev"
	commit  = "unknown"
)

const shutdownTimeout = 10 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "etcdflake:", err)
		os.Exit(1)
	}
}

// run starts the server and blocks until ctx is done or the worker stops.
// It returns nil on a requested shutdown.
func run(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("etcdflake", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to the JSON config file")
	showVersion := fs.Bool("version", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showVersion {
		_, err := fmt.Fprintf(stdout, "etcdflake %s (%s)\n", version, commit)
		return err
	}
	if *cfgPath == "" {
		return errors.New("-config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	log, err := newLogger(cfg.LogLevel)
	if err != nil {
		return err
	}
	defer log.Sync() //nolint:errcheck // nothing useful to do on failure at exit
	log.Info("starting", zap.String("version", version), zap.String("commit", commit))

	// Bind first: a taken or bad address should fail before we claim a worker ID.
	var lc net.ListenConfig
	grpcLis, err := lc.Listen(ctx, "tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}
	defer grpcLis.Close()
	httpLis, err := lc.Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen http: %w", err)
	}
	defer httpLis.Close()
	var debugLis net.Listener
	if cfg.DebugAddr != "" {
		if debugLis, err = lc.Listen(ctx, "tcp", cfg.DebugAddr); err != nil {
			return fmt.Errorf("listen debug: %w", err)
		}
		defer debugLis.Close()
	}

	cli, err := newEtcdClient(ctx, cfg.Etcd, log)
	if err != nil {
		return err
	}
	defer cli.Close()

	startCtx, cancel := context.WithTimeout(ctx, time.Minute)
	w, err := worker.Start(startCtx, worker.Config{Client: cli, MaxWorkers: cfg.MaxWorkers, Logger: log})
	cancel()
	if err != nil {
		return err
	}
	defer w.Stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "etcdflake_worker_id", Help: "Worker ID owned by this node.",
		}, func() float64 { return float64(w.ID()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "etcdflake_reservation_remaining_seconds",
			Help: "How long this node can keep issuing IDs without reaching etcd.",
		}, func() float64 { return w.ReservationRemaining().Seconds() }),
	)
	idServer, err := server.New(w, reg)
	if err != nil {
		return err
	}

	grpcServer, err := newGRPCServer(cfg)
	if err != nil {
		return err
	}
	etcdflakev1.RegisterIDServiceServer(grpcServer, idServer)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	httpServer := &http.Server{Handler: httpMux(reg, w), ReadHeaderTimeout: 5 * time.Second}
	var debugServer *http.Server
	if cfg.DebugAddr != "" {
		debugServer = &http.Server{Handler: debugMux(), ReadHeaderTimeout: 5 * time.Second}
	}

	serveErr := make(chan error, 3)
	go func() { serveErr <- grpcServer.Serve(grpcLis) }()
	go func() { serveErr <- httpServer.Serve(httpLis) }()
	if debugServer != nil {
		go func() { serveErr <- debugServer.Serve(debugLis) }()
	}
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(etcdflakev1.IDService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	log.Info("serving",
		zap.Int("worker_id", w.ID()),
		zap.Stringer("grpc_addr", grpcLis.Addr()),
		zap.Stringer("http_addr", httpLis.Addr()),
		zap.Bool("grpc_tls", cfg.GRPCTLS.Enabled()))

	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutdown requested")
	case <-w.Done():
		runErr = fmt.Errorf("worker stopped: %w", w.Err())
	case err := <-serveErr:
		runErr = fmt.Errorf("server failed: %w", err)
	}

	// Shut down in dependency order: stop advertising health, drain in-flight
	// RPCs while the worker can still issue IDs, then release the worker ID.
	healthServer.Shutdown()
	gracefulStop(grpcServer, shutdownTimeout)
	w.Stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	if debugServer != nil {
		_ = debugServer.Shutdown(shutdownCtx)
	}
	log.Info("stopped", zap.Error(runErr))
	return runErr
}

func gracefulStop(s *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		s.Stop()
	}
}

func newLogger(level string) (*zap.Logger, error) {
	lvl, err := zapcore.ParseLevel(level)
	if err != nil {
		return nil, err
	}
	zc := zap.NewProductionConfig()
	zc.Level = zap.NewAtomicLevelAt(lvl)
	zc.EncoderConfig.TimeKey = "time"
	zc.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	return zc.Build()
}

func newEtcdClient(ctx context.Context, c config.Etcd, log *zap.Logger) (*ec.Client, error) {
	ecfg := ec.Config{
		Endpoints:   c.Endpoints,
		DialTimeout: 5 * time.Second,
		Username:    c.Username,
		Password:    c.Password,
		Logger:      log.Named("etcd").WithOptions(zap.IncreaseLevel(zapcore.WarnLevel)),
	}
	if c.TLS.Enabled() {
		tc, err := etcdTLS(c.TLS)
		if err != nil {
			return nil, err
		}
		ecfg.TLS = tc
	}
	cli, err := ec.New(ecfg)
	if err != nil {
		return nil, fmt.Errorf("etcd client: %w", err)
	}
	cli.KV = namespace.NewKV(cli.KV, c.Prefix)
	cli.Watcher = namespace.NewWatcher(cli.Watcher, c.Prefix)
	cli.Lease = namespace.NewLease(cli.Lease, c.Prefix)

	// Fail fast if etcd is unreachable or we can't read under the prefix.
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := cli.Get(pctx, "workers/", ec.WithPrefix(), ec.WithCountOnly()); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("etcd check under prefix %q: %w", c.Prefix, err)
	}
	return cli, nil
}

func etcdTLS(t config.TLS) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if t.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("etcd client certificate: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	if t.CAFile != "" {
		pool, err := loadCertPool(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("etcd ca: %w", err)
		}
		tc.RootCAs = pool
	}
	return tc, nil
}

func newGRPCServer(cfg *config.Config) (*grpc.Server, error) {
	if !cfg.GRPCTLS.Enabled() {
		return grpc.NewServer(), nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.GRPCTLS.CertFile, cfg.GRPCTLS.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("grpc certificate: %w", err)
	}
	tc := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if cfg.GRPCTLS.ClientCAFile != "" {
		pool, err := loadCertPool(cfg.GRPCTLS.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("grpc client ca: %w", err)
		}
		tc.ClientCAs = pool
		tc.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return grpc.NewServer(grpc.Creds(credentials.NewTLS(tc))), nil
}

func loadCertPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path) //nolint:gosec // operator-provided path
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates in %s", path)
	}
	return pool, nil
}

func httpMux(reg *prometheus.Registry, w *worker.Worker) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /livez", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", func(rw http.ResponseWriter, _ *http.Request) {
		if w.Ready() {
			rw.WriteHeader(http.StatusOK)
			return
		}
		rw.WriteHeader(http.StatusServiceUnavailable)
	})
	return mux
}

func debugMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
