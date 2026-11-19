// Package worker coordinates snowflake workers through etcd.
//
// Each worker ID has two keys under the configured etcd prefix:
//
//	workers/live/<id>            attached to the owner's lease; exists while the ID is owned
//	workers/reserved_until/<id>  Unix ms; the owner never issues a timestamp above it
//
// The owner keeps the reservation a few seconds ahead of its clock and only
// issues IDs up to the reservation it has written. A new owner starts strictly
// above the stored reservation. So IDs stay unique across owners even when
// clocks disagree, the old owner is partitioned, or the process was paused.
// See docs/DESIGN.md.
package worker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pengubco/etcdflake/internal/snowflake"
	ec "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	liveKeyPrefix     = "workers/live/"
	reservedKeyPrefix = "workers/reserved_until/"
)

var (
	// ErrNotReady means the worker isn't serving: it is stopping or has stopped.
	ErrNotReady = errors.New("worker: not ready")

	// ErrNoWorkerID means every worker ID in [0, MaxWorkers) is taken or unusable.
	ErrNoWorkerID = errors.New("worker: no usable worker id")

	// ErrWorkerIDLost means the worker no longer owns its worker ID (lease expired or revoked).
	ErrWorkerIDLost = errors.New("worker: lost ownership of worker id")

	// ErrClockSkew means the wall clock is so far behind the issued IDs that the worker stopped.
	ErrClockSkew = errors.New("worker: clock skew too large")
)

// Config configures a Worker. Zero durations take the defaults below.
type Config struct {
	// Client is an etcd client, usually wrapped with a namespace for the cluster prefix.
	Client *ec.Client

	// MaxWorkers is the number of worker IDs, [0, MaxWorkers), shared by the cluster.
	MaxWorkers int

	// LeaseTTL is how long a crashed worker keeps its ID. Default 10s.
	LeaseTTL time.Duration

	// ReservationWindow is how far ahead of the clock the reservation is kept. Default 5s.
	ReservationWindow time.Duration

	// RenewInterval is how often the reservation is extended. Default 1s.
	RenewInterval time.Duration

	// MaxClockAhead is how far the issued timestamps may run ahead of the wall
	// clock, e.g. after a small backwards clock step. Default 1s.
	MaxClockAhead time.Duration

	// StopOnClockSkew stops the worker when issued timestamps are this far ahead
	// of the wall clock. A restart then claims a different worker ID. Default 5s.
	StopOnClockSkew time.Duration

	// Now returns the current Unix time in ms. Defaults to the wall clock.
	Now func() int64

	// Logger defaults to a no-op logger.
	Logger *zap.Logger
}

func (c *Config) setDefaults() error {
	if c.Client == nil {
		return errors.New("worker: nil etcd client")
	}
	if c.MaxWorkers < 1 || c.MaxWorkers > snowflake.MaxWorkerID+1 {
		return fmt.Errorf("worker: MaxWorkers %d out of range [1, %d]", c.MaxWorkers, snowflake.MaxWorkerID+1)
	}
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&c.LeaseTTL, 10*time.Second)
	def(&c.ReservationWindow, 5*time.Second)
	def(&c.RenewInterval, time.Second)
	def(&c.MaxClockAhead, time.Second)
	def(&c.StopOnClockSkew, 5*time.Second)
	if c.RenewInterval >= c.ReservationWindow {
		return fmt.Errorf("worker: RenewInterval %v must be shorter than ReservationWindow %v",
			c.RenewInterval, c.ReservationWindow)
	}
	if c.LeaseTTL < time.Second {
		return fmt.Errorf("worker: LeaseTTL %v shorter than 1s", c.LeaseTTL)
	}
	if c.Now == nil {
		c.Now = func() int64 { return time.Now().UnixMilli() }
	}
	if c.Logger == nil {
		c.Logger = zap.NewNop()
	}
	return nil
}

// Worker owns one worker ID and issues IDs for it.
type Worker struct {
	cfg     Config
	cli     *ec.Client
	log     *zap.Logger
	gen     *snowflake.Generator
	id      int
	leaseID ec.LeaseID

	ready    atomic.Bool
	cancel   context.CancelFunc // stops background goroutines
	bg       sync.WaitGroup
	stopOnce sync.Once
	done     chan struct{}
	err      error // why the worker stopped; set before done is closed
}

// Start claims a worker ID and starts serving. The ctx bounds startup only.
func Start(ctx context.Context, cfg Config) (*Worker, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	w := &Worker{cfg: cfg, cli: cfg.Client, log: cfg.Logger, done: make(chan struct{})}

	lease, err := w.cli.Grant(ctx, int64(cfg.LeaseTTL/time.Second))
	if err != nil {
		return nil, fmt.Errorf("worker: grant lease: %w", err)
	}
	w.leaseID = lease.ID

	bgCtx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	ok := false
	defer func() {
		if !ok {
			cancel()
			w.bg.Wait()
			w.revoke()
		}
	}()

	alive, err := w.cli.KeepAlive(bgCtx, w.leaseID)
	if err != nil {
		return nil, fmt.Errorf("worker: keep lease alive: %w", err)
	}

	if err := w.claim(ctx); err != nil {
		return nil, err
	}
	if err := w.extend(ctx); err != nil {
		return nil, fmt.Errorf("worker: initial reservation: %w", err)
	}

	w.bg.Go(func() { w.watchLease(bgCtx, alive) })
	w.bg.Go(func() { w.renewLoop(bgCtx) })
	w.ready.Store(true)
	ok = true
	w.log.Info("worker started", zap.Int("worker_id", w.id), zap.Int64("lease_id", int64(w.leaseID)))
	return w, nil
}

// ID returns the claimed worker ID.
func (w *Worker) ID() int { return w.id }

// NextID returns one ID.
func (w *Worker) NextID() (int64, error) {
	if !w.ready.Load() {
		return 0, ErrNotReady
	}
	id, err := w.gen.Next()
	return id, w.checkErr(err)
}

// NextIDs returns n increasing IDs.
func (w *Worker) NextIDs(n int) ([]int64, error) {
	if !w.ready.Load() {
		return nil, ErrNotReady
	}
	ids, err := w.gen.NextN(n)
	return ids, w.checkErr(err)
}

func (w *Worker) checkErr(err error) error {
	var skew *snowflake.ClockSkewError
	if errors.As(err, &skew) && time.Duration(skew.AheadMillis)*time.Millisecond >= w.cfg.StopOnClockSkew {
		// Stopping makes a fresh start pick a worker ID whose reservation is
		// behind this host's clock. Do it asynchronously so the request isn't blocked.
		go w.shutdown(fmt.Errorf("%w: %w", ErrClockSkew, err))
	}
	if errors.Is(err, snowflake.ErrStopped) {
		return ErrNotReady
	}
	return err
}

// Ready reports whether the worker can issue IDs now.
func (w *Worker) Ready() bool {
	return w.ready.Load() && w.gen.Limit() > w.cfg.Now()
}

// Done is closed when the worker has stopped.
func (w *Worker) Done() <-chan struct{} { return w.done }

// Err returns why the worker stopped: nil after a requested Stop. Valid after Done is closed.
func (w *Worker) Err() error {
	select {
	case <-w.done:
		return w.err
	default:
		return nil
	}
}

// ReservationRemaining is how long the worker can keep issuing IDs if etcd becomes unreachable now.
func (w *Worker) ReservationRemaining() time.Duration {
	return time.Duration(w.gen.Limit()-w.cfg.Now()) * time.Millisecond
}

// Stop stops issuing IDs, shrinks the reservation to the last issued
// timestamp so a successor can take over the ID right away, and releases the
// worker ID. It is safe to call more than once and concurrently.
func (w *Worker) Stop() {
	w.shutdown(nil)
}

func (w *Worker) shutdown(reason error) {
	w.stopOnce.Do(func() {
		w.ready.Store(false)
		last := w.gen.Freeze()
		w.cancel()
		w.bg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := w.cli.Txn(ctx).If(w.ownsID()).Then(
			ec.OpPut(reservedKey(w.id), strconv.FormatInt(last, 10)),
		).Commit(); err != nil {
			w.log.Warn("cannot shrink reservation", zap.Error(err))
		}
		w.revoke()

		w.err = reason
		if reason != nil {
			w.log.Error("worker stopped", zap.Int("worker_id", w.id), zap.Error(reason))
		} else {
			w.log.Info("worker stopped", zap.Int("worker_id", w.id))
		}
		close(w.done)
	})
}

func (w *Worker) revoke() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := w.cli.Revoke(ctx, w.leaseID); err != nil {
		w.log.Warn("cannot revoke lease", zap.Int64("lease_id", int64(w.leaseID)), zap.Error(err))
	}
}

// candidate is a worker ID that is free to claim.
type candidate struct {
	id          int
	reserved    int64 // stored reservation in Unix ms, 0 if none
	reservedRev int64 // ModRevision of the reservation key, 0 if absent
}

// claim takes the free worker ID with the oldest reservation, so the new
// owner is least likely to have to skip ahead of its own clock.
func (w *Worker) claim(ctx context.Context) error {
	cands, err := w.candidates(ctx)
	if err != nil {
		return err
	}
	now := w.cfg.Now()
	maxAhead := w.cfg.MaxClockAhead.Milliseconds()
	for _, c := range cands {
		if c.reserved-now >= maxAhead {
			// The previous owner's clock was ahead of ours. Claiming would mean
			// issuing timestamps too far in the future. Remaining candidates
			// have later reservations, so give up.
			w.log.Warn("free worker ids are reserved too far ahead of this clock",
				zap.Int("worker_id", c.id), zap.Int64("ahead_ms", c.reserved-now))
			break
		}
		// The reservation can't change while the live key is absent, but compare its
		// revision anyway so the claim is correct by construction.
		resp, err := w.cli.Txn(ctx).If(
			ec.Compare(ec.Version(liveKey(c.id)), "=", 0),
			ec.Compare(ec.ModRevision(reservedKey(c.id)), "=", c.reservedRev),
		).Then(
			ec.OpPut(liveKey(c.id), strconv.FormatInt(now, 10), ec.WithLease(w.leaseID)),
		).Commit()
		if err != nil {
			return fmt.Errorf("worker: claim id %d: %w", c.id, err)
		}
		if !resp.Succeeded {
			w.log.Debug("lost race for worker id", zap.Int("worker_id", c.id))
			continue
		}
		gen, err := snowflake.New(c.id, c.reserved, snowflake.Options{
			Now:            w.cfg.Now,
			MaxAheadMillis: maxAhead,
		})
		if err != nil {
			return err
		}
		w.id, w.gen = c.id, gen
		return nil
	}
	return fmt.Errorf("%w (max %d)", ErrNoWorkerID, w.cfg.MaxWorkers)
}

func (w *Worker) candidates(ctx context.Context) ([]candidate, error) {
	live, err := w.cli.Get(ctx, liveKeyPrefix, ec.WithPrefix(), ec.WithKeysOnly())
	if err != nil {
		return nil, fmt.Errorf("worker: list live ids: %w", err)
	}
	reserved, err := w.cli.Get(ctx, reservedKeyPrefix, ec.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("worker: list reservations: %w", err)
	}

	inUse := make(map[int]bool, len(live.Kvs))
	for _, kv := range live.Kvs {
		if id, ok := parseID(string(kv.Key), liveKeyPrefix); ok {
			inUse[id] = true
		}
	}
	res := make(map[int]candidate, len(reserved.Kvs))
	for _, kv := range reserved.Kvs {
		id, ok := parseID(string(kv.Key), reservedKeyPrefix)
		if !ok {
			continue
		}
		t, err := strconv.ParseInt(string(kv.Value), 10, 64)
		if err != nil {
			w.log.Warn("skip worker id with malformed reservation", zap.Int("worker_id", id), zap.ByteString("value", kv.Value))
			inUse[id] = true
			continue
		}
		res[id] = candidate{id: id, reserved: t, reservedRev: kv.ModRevision}
	}

	var cands []candidate
	for id := range w.cfg.MaxWorkers {
		if inUse[id] {
			continue
		}
		c, ok := res[id]
		if !ok {
			c = candidate{id: id}
		}
		cands = append(cands, c)
	}
	slices.SortFunc(cands, func(a, b candidate) int {
		return cmp.Or(cmp.Compare(a.reserved, b.reserved), cmp.Compare(a.id, b.id))
	})
	return cands, nil
}

// extend moves the reservation forward, then lets the generator use it.
// The order matters: the generator must never use a timestamp that isn't stored in etcd.
func (w *Worker) extend(ctx context.Context) error {
	now := w.cfg.Now()
	limit := max(now, w.gen.Last()) + w.cfg.ReservationWindow.Milliseconds()
	// Never lower the stored reservation below what the generator may already have used.
	limit = max(limit, w.gen.Limit())

	resp, err := w.cli.Txn(ctx).If(w.ownsID()).Then(
		ec.OpPut(reservedKey(w.id), strconv.FormatInt(limit, 10)),
	).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrWorkerIDLost
	}
	w.gen.SetLimit(limit)
	return nil
}

func (w *Worker) renewLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.RenewInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rctx, cancel := context.WithTimeout(ctx, w.cfg.RenewInterval)
		err := w.extend(rctx)
		cancel()
		switch {
		case err == nil:
		case errors.Is(err, ErrWorkerIDLost):
			go w.shutdown(err)
			return
		case ctx.Err() != nil:
			return
		default:
			// The generator keeps serving until the current reservation runs out.
			w.log.Warn("cannot extend reservation", zap.Error(err),
				zap.Duration("remaining", w.ReservationRemaining()))
		}
	}
}

func (w *Worker) watchLease(ctx context.Context, alive <-chan *ec.LeaseKeepAliveResponse) {
	// Drain keepalive responses so the etcd client doesn't log "channel full".
	for range alive {
	}
	if ctx.Err() == nil {
		// Run in a new goroutine: shutdown waits for this one to exit.
		go w.shutdown(fmt.Errorf("%w: lease keepalive ended", ErrWorkerIDLost))
	}
}

func (w *Worker) ownsID() ec.Cmp {
	return ec.Compare(ec.LeaseValue(liveKey(w.id)), "=", w.leaseID)
}

func liveKey(id int) string     { return liveKeyPrefix + strconv.Itoa(id) }
func reservedKey(id int) string { return reservedKeyPrefix + strconv.Itoa(id) }

func parseID(key, prefix string) (int, bool) {
	s, ok := strings.CutPrefix(key, prefix)
	if !ok {
		return 0, false
	}
	id, err := strconv.Atoi(s)
	return id, err == nil && id >= 0
}
