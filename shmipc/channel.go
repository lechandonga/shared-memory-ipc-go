package shmipc

import (
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Channel is one participant's handle to a named shared-memory channel. Safe
// for concurrent use by many goroutines in many processes simultaneously.
type Channel struct {
	cfg    Config
	region region
	file   interface{ Close() error }
	path   string

	mu         sync.Mutex
	closed     bool
	crashCount uint64
}

var (
	selfPID        = uint32(syscall.Getpid())
	selfStartTicks = procStartticks(int(selfPID))
)

const ownerWaitBudget = 2 * time.Millisecond

// Queue protocol (Vyukov-style bounded MPMC, extended with a slot state and
// owner identity for overwrite and crash recovery):
//
// Each slot carries a monotonically increasing sequence number measured in
// "turns": for slot i, the slot is writable when seq == turn (turn starts at
// i), and readable when seq == turn+1. Both head (writePos) and tail
// (readPos) are reserved with a single CAS before touching the slot, so a
// slot is ever mutated by the producer/consumer that owns its turn.
//
// state refines seq during a turn: EMPTY (writable), INFLIGHT (producer is
// copying), READY (readable), ABORTED (producer died). dataCRC guards the
// payload against a torn read during overwrite.
func futexWaitStep(deadline time.Time, hasDeadline bool) time.Duration {
	const maxStep = 50 * time.Millisecond
	if !hasDeadline {
		return maxStep
	}
	d := time.Until(deadline)
	switch {
	case d <= 0:
		return time.Nanosecond
	case d > maxStep:
		return maxStep
	default:
		return d
	}
}

func afterDeadline(deadline time.Time, has bool) bool {
	return has && !time.Now().Before(deadline)
}

// Send publishes msg. timeout == 0 non-blocking, timeout < 0 infinite.
func (ch *Channel) Send(msg []byte, timeout time.Duration) error {
	if uint32(len(msg)) > ch.cfg.PayloadSize {
		return ErrMessageTooLarge
	}
	hasDeadline := timeout >= 0
	deadline := time.Now().Add(timeout)
	capn := uint64(ch.cfg.Capacity)

	for {
		if ch.isClosed() {
			return ErrClosed
		}
		pos := atomic.LoadUint64(ch.region.u64(offWritePos))
		slot := ch.region.slot(pos, ch.cfg.Capacity, ch.cfg.slotSize())
		seq := atomic.LoadUint64(slot.u64(offSeq))
		state := atomic.LoadUint32(slot.u32(offState))

		// A producer that reserved this position and died before the seq
		// commit leaves INFLIGHT with a stale seq. Recover the slot for this
		// same position regardless of the seq value.
		if state == stateInflight && ch.slotDead(slot) {
			if atomic.LoadUint64(ch.region.u64(offWritePos)) == pos {
				if !atomic.CompareAndSwapUint64(ch.region.u64(offWritePos), pos, pos+1) {
					runtime.Gosched()
					continue
				}
				ch.fill(slot, pos, msg)
				return nil
			}
			// Head already moved past the crashed reservation: mark the
			// hole so readers skip it and the next lap can overwrite it.
			atomic.StoreUint32(slot.u32(offState), stateAborted)
		}

		state = atomic.LoadUint32(slot.u32(offState))
		dif := seq - pos

		switch {
		case dif == 0 && state != stateInflight:
			// Slot is writable for this turn. Reserve the turn first.
			if !atomic.CompareAndSwapUint64(ch.region.u64(offWritePos), pos, pos+1) {
				runtime.Gosched()
				continue
			}
			ch.fill(slot, pos, msg)
			return nil

		case dif == 0 && state == stateInflight:
			// Another producer reserved this turn and is copying (rare under
			// contention), or it died and left the slot poisoned.
			if ch.slotDead(slot) {
				if !atomic.CompareAndSwapUint64(ch.region.u64(offWritePos), pos, pos+1) {
					runtime.Gosched()
					continue
				}
				ch.fill(slot, pos, msg)
				return nil
			}
			if ch.waitOrFull(ch.region.u32(offRecverFutex), deadline, hasDeadline, timeout) {
				return ErrTimeout
			}
			if timeout == 0 {
				return ErrFull
			}

		default: // dif underflow: an unconsumed message occupies the slot.
			if ch.cfg.FullPolicy == FullPolicyOverwrite {
				if err := ch.evict(pos, slot, capn, msg, deadline, hasDeadline); err != nil {
					if err == errRetrySend {
						continue
					}
					return err
				}
				return nil
			}
			if timeout == 0 {
				return ErrFull
			}
			if ch.isClosed() {
				return ErrClosed
			}
			ch.waitSender(deadline, hasDeadline)
			if afterDeadline(deadline, hasDeadline) {
				return ErrTimeout
			}
		}
	}
}

// fill writes a slot already reserved for position pos.
func (ch *Channel) fill(slot region, pos uint64, msg []byte) {
	stateAddr := slot.u32(offState)
	atomic.StoreUint32(slot.u32(offWriterPID), selfPID)
	atomic.StoreUint64(slot.u64(offWriterTime), selfStartTicks)
	atomic.StoreUint32(stateAddr, stateInflight)

	crashAfterInflight(&ch.crashCount)

	copy(slot[offPayload:offPayload+len(msg)], msg)
	atomic.StoreUint64(slot.u64(offDataCRC), dataChecksum(msg))
	atomic.StoreUint32(slot.u32(offDataLen), uint32(len(msg)))
	// Commit: publish the readable turn, then the READY state.
	atomic.StoreUint64(slot.u64(offSeq), pos+1)
	atomic.StoreUint32(stateAddr, stateReady)
	ch.signalRecver()
}

// evict handles a full head slot under FullPolicyOverwrite.
func (ch *Channel) evict(pos uint64, slot region, capn uint64, msg []byte,
	deadline time.Time, hasDeadline bool) error {

	stateAddr := slot.u32(offState)
	if atomic.LoadUint32(stateAddr) == stateInflight {
		if ch.slotDead(slot) {
			atomic.StoreUint32(stateAddr, stateAborted)
		} else if !ch.budget(stateAddr, stateInflight, deadline, hasDeadline) {
			return ErrFull
		}
	}

	// Claim the new turn at head.
	if !atomic.CompareAndSwapUint64(ch.region.u64(offWritePos), pos, pos+1) {
		runtime.Gosched()
		return errRetrySend
	}

	// One message leaves the readable window for every message that enters
	// beyond capacity. Move the reader cursor up so the backlog never
	// exceeds capacity, and count exactly the evicted positions. The slot
	// we physically hold is the oldest one; its own message is included.
	for {
		rp := atomic.LoadUint64(ch.region.u64(offReadPos))
		if pos+1-rp <= capn {
			break
		}
		if !atomic.CompareAndSwapUint64(ch.region.u64(offReadPos), rp, rp+1) {
			runtime.Gosched()
		}
		atomic.AddUint64(ch.region.u64(offLostCount), 1)
	}

	// Rewrite the physical slot for the new turn. A concurrent reader of
	// the old message fails its seq+checksum validation and retries.
	atomic.StoreUint32(stateAddr, stateInflight)
	atomic.StoreUint32(slot.u32(offWriterPID), selfPID)
	atomic.StoreUint64(slot.u64(offWriterTime), selfStartTicks)
	copy(slot[offPayload:offPayload+len(msg)], msg)
	atomic.StoreUint64(slot.u64(offDataCRC), dataChecksum(msg))
	atomic.StoreUint32(slot.u32(offDataLen), uint32(len(msg)))
	atomic.StoreUint64(slot.u64(offSeq), pos+1)
	atomic.StoreUint32(stateAddr, stateReady)
	ch.signalRecver()
	return nil
}

func (ch *Channel) slotDead(slot region) bool {
	pid := atomic.LoadUint32(slot.u32(offWriterPID))
	start := atomic.LoadUint64(slot.u64(offWriterTime))
	return !slotOwnerAlive(pid, start)
}

func (ch *Channel) budget(addr *uint32, want uint32,
	deadline time.Time, hasDeadline bool) bool {
	step := ownerWaitBudget
	if hasDeadline {
		if d := time.Until(deadline); d < step {
			step = d
		}
	}
	if step <= 0 {
		return atomic.LoadUint32(addr) != want
	}
	futexWait32(addr, want, step)
	return atomic.LoadUint32(addr) != want
}

func (ch *Channel) waitOrFull(addr *uint32, deadline time.Time,
	hasDeadline bool, timeout time.Duration) bool {
	if timeout == 0 {
		return false
	}
	if afterDeadline(deadline, hasDeadline) {
		return true
	}
	futexWait32(addr, atomic.LoadUint32(addr), futexWaitStep(deadline, hasDeadline))
	return afterDeadline(deadline, hasDeadline)
}

func (ch *Channel) waitSender(deadline time.Time, hasDeadline bool) {
	futexWait32(ch.region.u32(offSenderFutex),
		atomic.LoadUint32(ch.region.u32(offSenderFutex)),
		futexWaitStep(deadline, hasDeadline))
}

func (ch *Channel) isClosed() bool { return loadClosed(ch.region) }

func (ch *Channel) signalRecver() {
	addr := ch.region.u32(offRecverFutex)
	atomic.AddUint32(addr, 1)
	futexWake32(addr, futexWakeAll)
}

func (ch *Channel) signalSender() {
	addr := ch.region.u32(offSenderFutex)
	atomic.AddUint32(addr, 1)
	futexWake32(addr, futexWakeAll)
}

// Recv consumes the next message; timeout == 0 non-blocking, < 0 infinite.
// Competing consumers share readPos and each message is delivered once.
func (ch *Channel) Recv(timeout time.Duration) (msg []byte, overflow *OverflowError, err error) {
	hasDeadline := timeout >= 0
	deadline := time.Now().Add(timeout)
	var lost uint64
	var crashLost uint64

	loss := func() *OverflowError {
		if n := atomic.SwapUint64(ch.region.u64(offLostCount), 0); n > 0 {
			lost += n
		}
		if lost > 0 {
			return &OverflowError{Lost: lost, Crash: crashLost > 0}
		}
		return nil
	}

	for {
		pos := atomic.LoadUint64(ch.region.u64(offReadPos))
		wp := atomic.LoadUint64(ch.region.u64(offWritePos))
		if pos == wp {
			if l := loss(); l != nil {
				return nil, l, nil
			}
			if ch.isClosed() {
				return nil, nil, ErrClosed
			}
			if timeout == 0 {
				return nil, nil, ErrEmpty
			}
			if afterDeadline(deadline, hasDeadline) {
				return nil, nil, ErrTimeout
			}
			futexWait32(ch.region.u32(offRecverFutex),
				atomic.LoadUint32(ch.region.u32(offRecverFutex)),
				futexWaitStep(deadline, hasDeadline))
			continue
		}

		slot := ch.region.slot(pos, ch.cfg.Capacity, ch.cfg.slotSize())
		seq := atomic.LoadUint64(slot.u64(offSeq))
		state := atomic.LoadUint32(slot.u32(offState))

		// A crashed producer leaves INFLIGHT at any sequence value; turn the
		// slot into an explicit hole before applying seq-based dispatch.
		if state == stateInflight && ch.slotDead(slot) {
			atomic.StoreUint32(slot.u32(offState), stateAborted)
			state = stateAborted
		}
		// A hole at the current position is always skipped, regardless of
		// whether the crashed producer managed to commit its seq turn.
		if state == stateAborted {
			if atomic.CompareAndSwapUint64(ch.region.u64(offReadPos), pos, pos+1) {
				atomic.StoreUint64(slot.u64(offSeq), pos+uint64(ch.cfg.Capacity))
				atomic.StoreUint32(slot.u32(offState), stateEmpty)
				lost++
				crashLost++
				ch.signalSender()
			}
			runtime.Gosched()
			continue
		}

		dif := seq - (pos + 1)

		switch {
		case dif == 0 && state == stateReady:
			n := atomic.LoadUint32(slot.u32(offDataLen))
			if n > ch.cfg.PayloadSize {
				runtime.Gosched()
				continue
			}
			payload := make([]byte, n)
			copy(payload, slot[offPayload:offPayload+n])
			crc := atomic.LoadUint64(slot.u64(offDataCRC))
			if atomic.LoadUint64(slot.u64(offSeq)) != pos+1 ||
				atomic.LoadUint32(slot.u32(offState)) != stateReady ||
				dataChecksum(payload) != crc {
				runtime.Gosched()
				continue
			}
			// Claim the turn; one consumer wins and advances the slot's seq
			// by capacity, making it writable for the next lap.
			if !atomic.CompareAndSwapUint64(ch.region.u64(offReadPos), pos, pos+1) {
				runtime.Gosched()
				continue
			}
			atomic.StoreUint64(slot.u64(offSeq), pos+uint64(ch.cfg.Capacity))
			atomic.StoreUint32(slot.u32(offState), stateEmpty)
			ch.signalSender()
			return payload, loss(), nil

		case dif == 0: // INFLIGHT (aborted is handled above the switch)
			if state == stateInflight && ch.slotDead(slot) {
				atomic.StoreUint32(slot.u32(offState), stateAborted)
				continue
			}
			if timeout == 0 {
				if l := loss(); l != nil {
					return nil, l, nil
				}
				return nil, nil, ErrEmpty
			}
			if afterDeadline(deadline, hasDeadline) {
				return nil, nil, ErrTimeout
			}
			futexWait32(ch.region.u32(offRecverFutex),
				atomic.LoadUint32(ch.region.u32(offRecverFutex)),
				futexWaitStep(deadline, hasDeadline))

		default:
			// seq is from a newer lap: this position was overwritten.
			if seq > pos+1 {
				if atomic.CompareAndSwapUint64(ch.region.u64(offReadPos), pos, pos+1) {
					lost++
				}
				runtime.Gosched()
				continue
			}
			// seq from an older lap while head advanced: transient; re-sync.
			if wp <= pos {
				runtime.Gosched()
				continue
			}
			if timeout == 0 {
				if l := loss(); l != nil {
					return nil, l, nil
				}
				return nil, nil, ErrEmpty
			}
			futexWait32(ch.region.u32(offRecverFutex),
				atomic.LoadUint32(ch.region.u32(offRecverFutex)),
				futexWaitStep(deadline, hasDeadline))
		}
	}
}

var errRetrySend = sentinelErr("retry send")

type sentinelErr string

func (e sentinelErr) Error() string { return string(e) }
