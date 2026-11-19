// Package snowflake generates 64-bit snowflake IDs.
//
// Layout (most significant bit first):
//
//	1 bit   always 0, so IDs are positive int64
//	41 bits milliseconds since Epoch (good until year 2094)
//	10 bits worker ID
//	12 bits sequence number within the millisecond
//
// A Generator never issues a timestamp above its limit. The limit is the
// time reservation the worker holds in etcd. A new owner of the same worker ID
// starts strictly above the previous owner's reservation, so two owners never
// issue the same ID, however far apart their clocks are.
package snowflake

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Epoch is the custom epoch, 2025-01-01T00:00:00Z, in Unix milliseconds.
const Epoch int64 = 1735689600000

// Bit layout.
const (
	TimestampBits = 41
	WorkerIDBits  = 10
	SequenceBits  = 12

	// MaxWorkerID is the largest worker ID that fits in WorkerIDBits.
	MaxWorkerID = 1<<WorkerIDBits - 1

	maxSequence  = 1<<SequenceBits - 1
	maxTimestamp = 1<<TimestampBits - 1
	workerShift  = SequenceBits
	timeShift    = WorkerIDBits + SequenceBits
)

var (
	// ErrReservationExhausted means the next ID would need a timestamp above the
	// reserved limit. It clears once the reservation is extended.
	ErrReservationExhausted = errors.New("snowflake: timestamp beyond reservation")

	// ErrStopped means the generator has been frozen and won't issue more IDs.
	ErrStopped = errors.New("snowflake: generator stopped")

	// ErrTimestampOutOfRange means the clock is before Epoch or past the 41-bit range.
	ErrTimestampOutOfRange = errors.New("snowflake: timestamp out of range")
)

// ClockSkewError means the generator's logical clock is ahead of the wall clock
// by more than the allowed tolerance. This happens when the wall clock jumps
// backwards, or when sustained load uses up sequence numbers faster than time passes.
type ClockSkewError struct {
	// AheadMillis is how far the logical clock is ahead of the wall clock.
	AheadMillis int64
}

func (e *ClockSkewError) Error() string {
	return fmt.Sprintf("snowflake: logical clock %dms ahead of wall clock", e.AheadMillis)
}

// Options configures a Generator.
type Options struct {
	// Now returns the current Unix time in milliseconds. Defaults to the wall clock.
	Now func() int64

	// MaxAheadMillis is how far the logical clock may run ahead of the wall clock.
	// While within this tolerance, a backwards clock jump or an exhausted
	// sequence is absorbed by continuing from the last timestamp, instead of
	// failing or blocking.
	MaxAheadMillis int64
}

// Generator issues snowflake IDs for one worker ID. It is safe for concurrent use.
type Generator struct {
	mu       sync.Mutex
	workerID int64
	now      func() int64
	maxAhead int64

	last    int64 // Unix ms of the last issued (or floor) timestamp
	seq     int64 // sequence number used at last
	limit   int64 // largest Unix ms timestamp that may be issued
	stopped bool
}

// New returns a Generator for workerID. All IDs it issues have a timestamp
// strictly greater than floor (Unix ms). The initial limit is 0, so nothing is
// issued until SetLimit is called.
func New(workerID int, floor int64, opts Options) (*Generator, error) {
	if workerID < 0 || workerID > MaxWorkerID {
		return nil, fmt.Errorf("snowflake: worker id %d out of range [0, %d]", workerID, MaxWorkerID)
	}
	if opts.Now == nil {
		opts.Now = func() int64 { return time.Now().UnixMilli() }
	}
	if opts.MaxAheadMillis < 0 {
		return nil, fmt.Errorf("snowflake: negative MaxAheadMillis %d", opts.MaxAheadMillis)
	}
	return &Generator{
		workerID: int64(workerID),
		now:      opts.Now,
		maxAhead: opts.MaxAheadMillis,
		last:     floor,
		seq:      maxSequence, // the next ID moves past floor
	}, nil
}

// WorkerID returns the generator's worker ID.
func (g *Generator) WorkerID() int { return int(g.workerID) }

// Next returns one ID.
func (g *Generator) Next() (int64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.next()
}

// NextN returns n IDs in increasing order. It either returns all n or an error.
func (g *Generator) NextN(n int) ([]int64, error) {
	if n < 0 {
		return nil, fmt.Errorf("snowflake: negative count %d", n)
	}
	ids := make([]int64, n)
	g.mu.Lock()
	defer g.mu.Unlock()
	for i := range ids {
		id, err := g.next()
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

func (g *Generator) next() (int64, error) {
	if g.stopped {
		return 0, ErrStopped
	}
	now := g.now()
	ts, seq := now, int64(0)
	if now <= g.last {
		ts, seq = g.last, g.seq+1
		if seq > maxSequence {
			ts, seq = g.last+1, 0
		}
	}
	if ahead := ts - now; ahead > g.maxAhead {
		return 0, &ClockSkewError{AheadMillis: ahead}
	}
	if ts > g.limit {
		return 0, ErrReservationExhausted
	}
	rel := ts - Epoch
	if rel < 0 || rel > maxTimestamp {
		return 0, ErrTimestampOutOfRange
	}
	g.last, g.seq = ts, seq
	return rel<<timeShift | g.workerID<<workerShift | seq, nil
}

// Last returns the Unix ms timestamp of the most recently issued ID, or the floor.
func (g *Generator) Last() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.last
}

// Limit returns the current reservation limit in Unix ms.
func (g *Generator) Limit() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limit
}

// SetLimit raises the reservation limit. A lower value is ignored, because
// shrinking the limit after IDs were issued under it gains nothing.
func (g *Generator) SetLimit(limit int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if limit > g.limit {
		g.limit = limit
	}
}

// Freeze stops the generator permanently and returns the last issued timestamp.
// After Freeze, it is safe to shrink the persisted reservation to the returned value.
func (g *Generator) Freeze() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stopped = true
	return g.last
}

// Decompose splits an ID into its Unix ms timestamp, worker ID and sequence number.
func Decompose(id int64) (unixMillis int64, workerID int, seq int) {
	return id>>timeShift + Epoch, int(id >> workerShift & MaxWorkerID), int(id & maxSequence)
}
