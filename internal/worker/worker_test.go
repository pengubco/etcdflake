package worker

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pengubco/etcdflake/internal/etcdtest"
	"github.com/pengubco/etcdflake/internal/snowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ec "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"go.uber.org/zap/zaptest"
)

const prefix = "/etcdflake-test/"

func nsClient(t *testing.T, srv *etcdtest.Server) *ec.Client {
	t.Helper()
	cli := srv.Client(t)
	cli.KV = namespace.NewKV(cli.KV, prefix)
	cli.Watcher = namespace.NewWatcher(cli.Watcher, prefix)
	cli.Lease = namespace.NewLease(cli.Lease, prefix)
	return cli
}

func testConfig(t *testing.T, cli *ec.Client, maxWorkers int) Config {
	return Config{
		Client:            cli,
		MaxWorkers:        maxWorkers,
		LeaseTTL:          2 * time.Second,
		ReservationWindow: 500 * time.Millisecond,
		RenewInterval:     100 * time.Millisecond,
		Logger:            zaptest.NewLogger(t),
	}
}

func start(t *testing.T, cfg Config) *Worker {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, err := Start(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(w.Stop)
	return w
}

func getInt(t *testing.T, cli *ec.Client, key string) (int64, bool) {
	t.Helper()
	resp, err := cli.Get(context.Background(), key)
	require.NoError(t, err)
	if resp.Count == 0 {
		return 0, false
	}
	v, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	require.NoError(t, err)
	return v, true
}

func TestConfigValidation(t *testing.T) {
	srv := etcdtest.Start(t)
	cli := nsClient(t, srv)
	for _, cfg := range []Config{
		{MaxWorkers: 1},
		{Client: cli, MaxWorkers: 0},
		{Client: cli, MaxWorkers: snowflake.MaxWorkerID + 2},
		{Client: cli, MaxWorkers: 1, RenewInterval: time.Second, ReservationWindow: time.Second},
		{Client: cli, MaxWorkers: 1, LeaseTTL: time.Millisecond},
	} {
		_, err := Start(context.Background(), cfg)
		assert.Error(t, err, "%+v", cfg)
	}
}

func TestWorkersClaimDistinctIDs(t *testing.T) {
	srv := etcdtest.Start(t)
	cli := nsClient(t, srv)

	w0 := start(t, testConfig(t, cli, 2))
	w1 := start(t, testConfig(t, cli, 2))
	assert.ElementsMatch(t, []int{0, 1}, []int{w0.ID(), w1.ID()})

	_, err := Start(context.Background(), testConfig(t, cli, 2))
	assert.ErrorIs(t, err, ErrNoWorkerID)

	for _, w := range []*Worker{w0, w1} {
		id, err := w.NextID()
		require.NoError(t, err)
		_, wid, _ := snowflake.Decompose(id)
		assert.Equal(t, w.ID(), wid)
		assert.True(t, w.Ready())
	}
}

func TestConcurrentStartsClaimDistinctIDs(t *testing.T) {
	srv := etcdtest.Start(t)
	const n = 8
	ids := make(chan int, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			w := start(t, testConfig(t, nsClient(t, srv), n))
			ids <- w.ID()
		})
	}
	wg.Wait()
	close(ids)
	seen := map[int]bool{}
	for id := range ids {
		assert.False(t, seen[id], "worker id %d claimed twice", id)
		seen[id] = true
	}
	assert.Len(t, seen, n)
}

func TestReservationIsPersistedBeforeUse(t *testing.T) {
	srv := etcdtest.Start(t)
	cli := nsClient(t, srv)
	w := start(t, testConfig(t, cli, 1))

	for range 20 {
		id, err := w.NextID()
		require.NoError(t, err)
		ts, _, _ := snowflake.Decompose(id)
		reserved, ok := getInt(t, cli, reservedKey(w.ID()))
		require.True(t, ok)
		assert.LessOrEqual(t, ts, reserved)
		time.Sleep(20 * time.Millisecond)
	}
	assert.Positive(t, w.ReservationRemaining())
}

func TestStopReleasesIDAndSuccessorContinuesAbove(t *testing.T) {
	srv := etcdtest.Start(t)
	cli := nsClient(t, srv)

	old := start(t, testConfig(t, cli, 1))
	ids, err := old.NextIDs(100)
	require.NoError(t, err)
	lastOld := ids[len(ids)-1]
	old.Stop()
	old.Stop() // idempotent
	<-old.Done()
	assert.NoError(t, old.Err())
	_, err = old.NextID()
	assert.ErrorIs(t, err, ErrNotReady)

	reserved, ok := getInt(t, cli, reservedKey(0))
	require.True(t, ok)
	lastTS, _, _ := snowflake.Decompose(lastOld)
	assert.Equal(t, lastTS, reserved, "stop shrinks the reservation to the last issued timestamp")

	next := start(t, testConfig(t, cli, 1))
	assert.Equal(t, 0, next.ID(), "the released id is reusable immediately")
	id, err := next.NextID()
	require.NoError(t, err)
	assert.Greater(t, id, lastOld)
	ts, _, _ := snowflake.Decompose(id)
	assert.Greater(t, ts, reserved)
}

func TestSuccessorStartsAboveReservationEvenWithSlowClock(t *testing.T) {
	srv := etcdtest.Start(t)
	cli := nsClient(t, srv)

	// A previous owner whose clock was 300ms ahead left a reservation.
	reserved := time.Now().UnixMilli() + 300
	_, err := cli.Put(context.Background(), reservedKey(0), strconv.FormatInt(reserved, 10))
	require.NoError(t, err)

	w := start(t, testConfig(t, cli, 1))
	require.Equal(t, 0, w.ID())
	id, err := w.NextID()
	require.NoError(t, err)
	ts, _, _ := snowflake.Decompose(id)
	assert.Greater(t, ts, reserved)
}

func TestSkipsIDReservedFarAhead(t *testing.T) {
	srv := etcdtest.Start(t)
	cli := nsClient(t, srv)

	ahead := time.Now().Add(time.Hour).UnixMilli()
	_, err := cli.Put(context.Background(), reservedKey(0), strconv.FormatInt(ahead, 10))
	require.NoError(t, err)

	w := start(t, testConfig(t, cli, 2))
	assert.Equal(t, 1, w.ID())

	_, err = Start(context.Background(), testConfig(t, cli, 2))
	assert.ErrorIs(t, err, ErrNoWorkerID)
}

func TestMalformedReservationIsSkipped(t *testing.T) {
	srv := etcdtest.Start(t)
	cli := nsClient(t, srv)
	_, err := cli.Put(context.Background(), reservedKey(0), "garbage")
	require.NoError(t, err)

	w := start(t, testConfig(t, cli, 2))
	assert.Equal(t, 1, w.ID())
}

func TestPrefersOldestReservation(t *testing.T) {
	srv := etcdtest.Start(t)
	cli := nsClient(t, srv)
	now := time.Now().UnixMilli()
	_, err := cli.Put(context.Background(), reservedKey(0), strconv.FormatInt(now-1000, 10))
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), reservedKey(1), strconv.FormatInt(now-5000, 10))
	require.NoError(t, err)

	w := start(t, testConfig(t, cli, 3))
	assert.Equal(t, 2, w.ID(), "an id that was never used comes first")
	w = start(t, testConfig(t, cli, 3))
	assert.Equal(t, 1, w.ID())
}

func TestLosingLeaseStopsWorker(t *testing.T) {
	srv := etcdtest.Start(t)
	cli := nsClient(t, srv)
	w := start(t, testConfig(t, cli, 1))

	// Simulate another party revoking the lease, as if it expired.
	_, err := cli.Revoke(context.Background(), w.leaseID)
	require.NoError(t, err)

	select {
	case <-w.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after losing its lease")
	}
	assert.ErrorIs(t, w.Err(), ErrWorkerIDLost)
	assert.False(t, w.Ready())
	_, err = w.NextID()
	assert.ErrorIs(t, err, ErrNotReady)
}

func TestKeepsServingWithinReservationWhenEtcdIsDown(t *testing.T) {
	srv := etcdtest.Start(t)
	cli := nsClient(t, srv)
	cfg := testConfig(t, cli, 1)
	cfg.ReservationWindow = 2 * time.Second
	cfg.LeaseTTL = 5 * time.Second
	w := start(t, cfg)

	_, err := w.NextID()
	require.NoError(t, err)
	go srv.Stop() // Close is slow; the reservation must cover the gap
	time.Sleep(50 * time.Millisecond)
	_, err = w.NextID()
	assert.NoError(t, err, "serves from the reservation while etcd is going away")

	require.Eventually(t, func() bool {
		_, err := w.NextID()
		return err != nil
	}, 20*time.Second, 50*time.Millisecond)
	_, err = w.NextID()
	assert.ErrorIs(t, err, snowflake.ErrReservationExhausted)
	assert.False(t, w.Ready())
}

func TestClockSkewStopsWorker(t *testing.T) {
	srv := etcdtest.Start(t)
	cli := nsClient(t, srv)

	var mu sync.Mutex
	offset := int64(0)
	cfg := testConfig(t, cli, 1)
	cfg.Now = func() int64 {
		mu.Lock()
		defer mu.Unlock()
		return time.Now().UnixMilli() + offset
	}
	w := start(t, cfg)
	_, err := w.NextID()
	require.NoError(t, err)

	// A small step back is absorbed.
	mu.Lock()
	offset = -200
	mu.Unlock()
	_, err = w.NextID()
	require.NoError(t, err)

	// A large step back stops the worker.
	mu.Lock()
	offset = -10_000
	mu.Unlock()
	_, err = w.NextID()
	var skew *snowflake.ClockSkewError
	require.ErrorAs(t, err, &skew)
	select {
	case <-w.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop on clock skew")
	}
	assert.ErrorIs(t, w.Err(), ErrClockSkew)
}

func TestIDsIncreaseAcrossManyHandovers(t *testing.T) {
	srv := etcdtest.Start(t)
	cli := nsClient(t, srv)
	var last int64
	for range 5 {
		w := start(t, testConfig(t, cli, 1))
		ids, err := w.NextIDs(1000)
		require.NoError(t, err)
		assert.Greater(t, ids[0], last)
		last = ids[len(ids)-1]
		w.Stop()
	}
}
