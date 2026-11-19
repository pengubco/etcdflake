package snowflake

import (
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock is a manually advanced clock in Unix ms.
type fakeClock struct {
	mu sync.Mutex
	t  int64
}

func (c *fakeClock) Now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Set(t int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

const t0 = Epoch + 1_000_000

func newTestGen(t *testing.T, workerID int, floor int64, clk *fakeClock, maxAhead int64) *Generator {
	t.Helper()
	g, err := New(workerID, floor, Options{Now: clk.Now, MaxAheadMillis: maxAhead})
	require.NoError(t, err)
	g.SetLimit(1 << 62)
	return g
}

func TestNewValidatesWorkerID(t *testing.T) {
	for _, id := range []int{-1, MaxWorkerID + 1} {
		_, err := New(id, 0, Options{})
		assert.Error(t, err, "worker id %d", id)
	}
	for _, id := range []int{0, 1, MaxWorkerID} {
		_, err := New(id, 0, Options{})
		assert.NoError(t, err, "worker id %d", id)
	}
	_, err := New(0, 0, Options{MaxAheadMillis: -1})
	assert.Error(t, err)
}

func TestLayout(t *testing.T) {
	clk := &fakeClock{t: t0}
	g := newTestGen(t, 7, 0, clk, 0)

	id, err := g.Next()
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000)<<22|7<<12, id)

	id, err = g.Next()
	require.NoError(t, err)
	ts, w, seq := Decompose(id)
	assert.Equal(t, t0, ts)
	assert.Equal(t, 7, w)
	assert.Equal(t, 1, seq)
}

func TestSequenceResetsOnNewMillisecond(t *testing.T) {
	clk := &fakeClock{t: t0}
	g := newTestGen(t, 1, 0, clk, 0)
	_, _ = g.Next()
	_, _ = g.Next()
	clk.Set(t0 + 1)
	id, err := g.Next()
	require.NoError(t, err)
	ts, _, seq := Decompose(id)
	assert.Equal(t, t0+1, ts)
	assert.Equal(t, 0, seq)
}

func TestSequenceExhaustionBorrowsNextMillisecond(t *testing.T) {
	clk := &fakeClock{t: t0}
	g := newTestGen(t, 1, 0, clk, 1)
	ids, err := g.NextN(maxSequence + 1)
	require.NoError(t, err)
	ts, _, seq := Decompose(ids[len(ids)-1])
	assert.Equal(t, t0, ts)
	assert.Equal(t, maxSequence, seq)

	// Sequence exhausted at t0: continue at t0+1, which is 1ms ahead and allowed.
	id, err := g.Next()
	require.NoError(t, err)
	ts, _, seq = Decompose(id)
	assert.Equal(t, t0+1, ts)
	assert.Equal(t, 0, seq)
}

func TestSequenceExhaustionWithoutToleranceFails(t *testing.T) {
	clk := &fakeClock{t: t0}
	g := newTestGen(t, 1, 0, clk, 0)
	_, err := g.NextN(maxSequence + 1)
	require.NoError(t, err)
	_, err = g.Next()
	var skew *ClockSkewError
	require.ErrorAs(t, err, &skew)
	assert.Equal(t, int64(1), skew.AheadMillis)

	clk.Set(t0 + 1)
	_, err = g.Next()
	assert.NoError(t, err)
}

func TestClockBackwardsWithinTolerance(t *testing.T) {
	clk := &fakeClock{t: t0}
	g := newTestGen(t, 1, 0, clk, 100)
	prev, err := g.Next()
	require.NoError(t, err)

	clk.Set(t0 - 50)
	id, err := g.Next()
	require.NoError(t, err)
	assert.Greater(t, id, prev)
	ts, _, _ := Decompose(id)
	assert.Equal(t, t0, ts, "keeps issuing at the last timestamp")
}

func TestClockBackwardsBeyondTolerance(t *testing.T) {
	clk := &fakeClock{t: t0}
	g := newTestGen(t, 1, 0, clk, 100)
	_, err := g.Next()
	require.NoError(t, err)

	clk.Set(t0 - 1000)
	_, err = g.Next()
	var skew *ClockSkewError
	require.ErrorAs(t, err, &skew)
	assert.Equal(t, int64(1000), skew.AheadMillis)

	// A failed call must not change state.
	clk.Set(t0 + 1)
	id, err := g.Next()
	require.NoError(t, err)
	ts, _, seq := Decompose(id)
	assert.Equal(t, t0+1, ts)
	assert.Equal(t, 0, seq)
}

func TestFloorIsExclusive(t *testing.T) {
	clk := &fakeClock{t: t0}
	g := newTestGen(t, 1, t0, clk, 10)
	id, err := g.Next()
	require.NoError(t, err)
	ts, _, _ := Decompose(id)
	assert.Equal(t, t0+1, ts, "must start strictly above floor")
}

func TestFloorAheadOfClockBeyondTolerance(t *testing.T) {
	clk := &fakeClock{t: t0}
	g := newTestGen(t, 1, t0+5000, clk, 10)
	_, err := g.Next()
	var skew *ClockSkewError
	assert.ErrorAs(t, err, &skew)
}

func TestLimit(t *testing.T) {
	clk := &fakeClock{t: t0}
	g, err := New(1, 0, Options{Now: clk.Now})
	require.NoError(t, err)

	_, err = g.Next()
	assert.ErrorIs(t, err, ErrReservationExhausted, "limit starts at 0")

	g.SetLimit(t0)
	_, err = g.Next()
	require.NoError(t, err)

	clk.Set(t0 + 1)
	_, err = g.Next()
	assert.ErrorIs(t, err, ErrReservationExhausted)

	g.SetLimit(t0 - 100) // lowering is ignored
	assert.Equal(t, t0, g.Limit())
}

func TestFreeze(t *testing.T) {
	clk := &fakeClock{t: t0}
	g := newTestGen(t, 1, 0, clk, 0)
	_, err := g.Next()
	require.NoError(t, err)
	assert.Equal(t, t0, g.Freeze())
	_, err = g.Next()
	assert.ErrorIs(t, err, ErrStopped)
}

func TestTimestampOutOfRange(t *testing.T) {
	clk := &fakeClock{t: Epoch - 1}
	g := newTestGen(t, 1, 0, clk, 0)
	_, err := g.Next()
	assert.ErrorIs(t, err, ErrTimestampOutOfRange)

	clk.Set(Epoch + maxTimestamp + 1)
	_, err = g.Next()
	assert.ErrorIs(t, err, ErrTimestampOutOfRange)

	clk.Set(Epoch + maxTimestamp)
	id, err := g.Next()
	require.NoError(t, err)
	assert.Positive(t, id)
}

func TestNextNNegative(t *testing.T) {
	g := newTestGen(t, 1, 0, &fakeClock{t: t0}, 0)
	_, err := g.NextN(-1)
	assert.Error(t, err)
}

func TestConcurrentUniqueAndIncreasing(t *testing.T) {
	g, err := New(3, 0, Options{MaxAheadMillis: 1000})
	require.NoError(t, err)
	g.SetLimit(1 << 62)

	const goroutines, perG = 8, 5000
	results := make([][]int64, goroutines)
	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Go(func() {
			for range perG {
				id, err := g.Next()
				if err != nil {
					t.Error(err)
					return
				}
				results[i] = append(results[i], id)
			}
		})
	}
	wg.Wait()

	seen := make(map[int64]struct{}, goroutines*perG)
	var all []int64
	for _, r := range results {
		assert.True(t, sort.SliceIsSorted(r, func(a, b int) bool { return r[a] < r[b] }),
			"IDs seen by one goroutine must increase")
		for _, id := range r {
			if _, dup := seen[id]; dup {
				t.Fatalf("duplicate id %d", id)
			}
			seen[id] = struct{}{}
			all = append(all, id)
		}
	}
	assert.Len(t, all, goroutines*perG)
}

func TestClockSkewErrorIsMatchable(t *testing.T) {
	var err error = &ClockSkewError{AheadMillis: 5}
	var skew *ClockSkewError
	assert.True(t, errors.As(err, &skew))
	assert.Contains(t, err.Error(), "5ms")
}
