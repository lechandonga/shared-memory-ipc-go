//go:build linux && (amd64 || arm64)

package shmipc

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"
)

// Create creates and initializes a named channel.
//
// The name maps to /dev/shm/shmipc-<name>; only [A-Za-z0-9._-] characters are
// allowed. ErrAlreadyExists is returned when the name is taken; stale backing
// files whose creator process is dead are treated as not existing and
// reclaimed. The returned [Config] is normalized and must be reused by
// attachers verbatim.
func Create(name string, cfg Config) (*Channel, Config, error) {
	if !validName(name) {
		return nil, Config{}, ErrInvalidName
	}
	if err := cfg.normalize(); err != nil {
		return nil, Config{}, err
	}
	path := shmPath(name)
	total := cfg.totalSize()

	f, err := createFile(path, int(total))
	if err == ErrAlreadyExists {
		// Possibly a stale channel from a crashed creator.
		if reclaimStale(path) {
			f, err = createFile(path, int(total))
		}
	}
	if err != nil {
		return nil, Config{}, err
	}

	r, err := mapRegion(f, int(total))
	if err != nil {
		f.Close()
		os.Remove(path)
		return nil, Config{}, err
	}

	r.writeHeaderStatic(cfg, total)
	clear(r[headerSize-48 : headerSize])
	atomic.StoreUint32(r.u32(offClosed), 0)
	atomic.StoreUint64(r.u64(offWritePos), 0)
	atomic.StoreUint64(r.u64(offReadPos), 0)
	atomic.StoreUint32(r.u32(offCreatorPID), selfPID)
	atomic.StoreUint64(r.u64(offCreatorBoot), selfStartTicks)

	// Every slot starts free (generation 0); generations advance as
	// positions are published and reset to 0 when consumed.
	for i := uint32(0); i < cfg.Capacity; i++ {
		s := r.slot(uint64(i), cfg.Capacity, cfg.slotSize())
		// Vyukov initial sequence: slot i is writable at turn i.
		atomic.StoreUint64(s.u64(offSeq), uint64(i))
		atomic.StoreUint32(s.u32(offState), stateEmpty)
		atomic.StoreUint32(s.u32(offDataLen), 0)
	}
	r.storeHeaderCRC()

	return &Channel{cfg: cfg, region: r, file: f, path: path}, cfg, nil
}

// Attach opens an existing channel. The requested cfg is validated field by
// field against the creator's layout; any mismatch returns ErrIncompatible
// wrapped around a specific reason (see Reason* variables), so callers can
// distinguish ABI drift, wrong capacity, wrong payload size, truncation or
// corruption instead of risking a silent misread.
func Attach(name string, cfg Config) (*Channel, error) {
	if !validName(name) {
		return nil, ErrInvalidName
	}
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	path := shmPath(name)
	f, err := openFile(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	// Mapping beyond the backing file would SIGBUS on first touch; reject
	// truncated files before mapping.
	if fi.Size() < int64(cfg.totalSize()) {
		f.Close()
		return nil, ReasonTruncated
	}

	r, err := mapRegion(f, int(cfg.totalSize()))
	if err != nil {
		f.Close()
		return nil, err
	}

	if err := verifyHeader(r, cfg, uint64(fi.Size())); err != nil {
		unmapRegion(r)
		f.Close()
		return nil, err
	}
	return &Channel{cfg: cfg, region: r, file: f, path: path}, nil
}

func verifyHeader(r region, cfg Config, fileSize uint64) error {
	if fileSize < headerSize {
		return ReasonTruncated
	}
	if g := binary.LittleEndian.Uint32(r[offMagic:]); g != magic {
		return ReasonBadMagic
	}
	if g := binary.LittleEndian.Uint16(r[offVersion:]); g != layoutVersion {
		return fmt.Errorf("%w: got layout version %d, want %d",
			ReasonBadVersion, g, layoutVersion)
	}
	if g := binary.LittleEndian.Uint32(r[offNumSlots:]); g != cfg.Capacity {
		return fmt.Errorf("%w: got capacity %d, want %d",
			ReasonCapacityMismatch, g, cfg.Capacity)
	}
	if g := binary.LittleEndian.Uint32(r[offPayloadSize:]); g != cfg.PayloadSize {
		return fmt.Errorf("%w: got payload size %d, want %d",
			ReasonPayloadMismatch, g, cfg.PayloadSize)
	}
	if g := binary.LittleEndian.Uint32(r[offSlotSize:]); g != cfg.slotSize() ||
		binary.LittleEndian.Uint64(r[offTotalSize:]) != cfg.totalSize() {
		return ReasonSizeMismatch
	}
	if fileSize < cfg.totalSize() {
		return ReasonTruncated
	}
	if binary.LittleEndian.Uint32(r[offHeaderCRC:]) != r.headerCRC() {
		return ReasonCorruptHeader
	}
	return nil
}

// Close marks the channel closed and wakes all waiters. Closed channels
// reject new Sends; Recv first drains already published messages and then
// returns ErrClosed. Close is idempotent and may be called by any participant.
// The backing file is removed only by [Channel.Destroy].
func (ch *Channel) Close() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.closed {
		return nil
	}
	ch.closed = true
	atomic.StoreUint32(ch.region.u32(offClosed), 1)
	futexWake32(ch.region.u32(offRecverFutex), futexWakeAll)
	futexWake32(ch.region.u32(offSenderFutex), futexWakeAll)
	return nil
}

// Destroy closes this handle and removes the backing file. Other processes
// keep their existing mappings usable until they detach, but no new attaches
// will succeed. Typically the creator calls Destroy after all participants
// have detached; a crashed creator's stale file is reclaimed automatically by
// a later Create of the same name.
func (ch *Channel) Destroy() error {
	atomic.StoreUint32(ch.region.u32(offClosed), 1)
	futexWake32(ch.region.u32(offRecverFutex), futexWakeAll)
	futexWake32(ch.region.u32(offSenderFutex), futexWakeAll)

	var firstErr error
	if err := unmapRegion(ch.region); err != nil {
		firstErr = err
	}
	if ch.file != nil {
		if err := ch.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := os.Remove(ch.path); err != nil && !os.IsNotExist(err) && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Detach unmaps the local handle without changing channel state. The channel
// remains usable by other processes.
func (ch *Channel) Detach() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.region == nil {
		return nil
	}
	err := unmapRegion(ch.region)
	if ch.file != nil {
		if cerr := ch.file.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	ch.region = nil
	return err
}

func validName(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '.' || c == '_' || c == '-':
		default:
			return false
		}
	}
	return name != "." && name != ".."
}

// reclaimStale removes the backing file only when its recorded creator is
// dead (PID + starttime identity). A live creator's file is left untouched.
func reclaimStale(path string) bool {
	f, err := openFile(path)
	if err != nil {
		return false
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || fi.Size() < headerSize {
		return os.Remove(path) == nil
	}
	r, err := mapRegion(f, headerSize)
	if err != nil {
		return false
	}
	defer unmapRegion(r)

	pid := atomic.LoadUint32(r.u32(offCreatorPID))
	boot := atomic.LoadUint64(r.u64(offCreatorBoot))
	if slotOwnerAlive(pid, boot) {
		return false
	}
	return os.Remove(path) == nil
}
