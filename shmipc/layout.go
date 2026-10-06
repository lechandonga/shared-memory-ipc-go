package shmipc

import (
	"encoding/binary"
	"hash/crc32"
	"unsafe"
)

// Binary layout constants. Bumping layoutVersion changes the ABI and causes
// Attach to reject older/newer channels with [ReasonBadVersion].
const (
	magic         uint32 = 0x5348_4D43 // "SHMC"
	layoutVersion uint16 = 1

	headerSize = 128

	// Fixed offsets inside the header. Keeping explicit offsets makes the
	// on-disk/in-shm layout independent of the compiler's struct padding.
	offMagic       = 0
	offVersion     = 4
	offFlags       = 6
	offTotalSize   = 8
	offNumSlots    = 16
	offSlotSize    = 20
	offPayloadSize = 24
	offHeaderCRC   = 28
	offWritePos    = 32
	offReadPos     = 40
	offClosed      = 48
	offFullPolicy  = 52
	offSenderFutex = 56 // must be 4-byte aligned for futex
	offRecverFutex = 60 // must be 4-byte aligned for futex
	offCreatorPID  = 64
	offCreatorBoot = 72
	offLostCount   = 80 // uint64: evicted (overwritten/crashed) messages
	// header ends at 88; remaining bytes up to headerSize are reserved.
)

const (
	// Slot fixed fields occupy the first 48 bytes; the rest is payload.
	slotHdrSize = 48

	offSeq        = 0  // uint64: Vyukov-style sequence number
	offState      = 8  // uint32
	offDataLen    = 12 // uint32
	offWriterPID  = 16 // uint32
	offWriterTime = 24 // uint64: owner starttime in clock ticks (PID reuse guard)
	offWriterGen  = 32 // reserved (kept for ABI alignment)
	offDataCRC    = 40 // uint64: fnv-1a checksum over payload
	offPayload    = 48
)

// Slot publication state machine.
const (
	stateEmpty    uint32 = 0 // slot free (seq == position)
	stateInflight uint32 = 1 // a writer owns the slot, payload incomplete
	stateReady    uint32 = 2 // payload complete, awaiting consumption
	stateAborted  uint32 = 3 // writer died mid-publish; message is a hole
)

type region []byte

func (r region) u32(off int) *uint32 {
	return (*uint32)(unsafe.Pointer(&r[off]))
}
func (r region) u64(off int) *uint64 {
	return (*uint64)(unsafe.Pointer(&r[off]))
}

func (r region) headerCRC() uint32 {
	crc := crc32.NewIEEE()
	// Only static layout/identity fields are checksummed. Mutable runtime
	// state (writePos, readPos, closed, lost count, futex words) changes
	// after creation and is protected by atomicity instead.
	crc.Write(r[offMagic:offHeaderCRC])
	crc.Write(r[offFullPolicy : offFullPolicy+4])
	crc.Write(r[offCreatorPID : offCreatorPID+4])
	crc.Write(r[offCreatorBoot : offCreatorBoot+8])
	return crc.Sum32()
}

func (r region) writeHeaderStatic(cfg Config, totalSize uint64) {
	binary.LittleEndian.PutUint32(r[offMagic:], magic)
	binary.LittleEndian.PutUint16(r[offVersion:], layoutVersion)
	binary.LittleEndian.PutUint16(r[offFlags:], 0)
	binary.LittleEndian.PutUint64(r[offTotalSize:], totalSize)
	binary.LittleEndian.PutUint32(r[offNumSlots:], cfg.Capacity)
	binary.LittleEndian.PutUint32(r[offSlotSize:], cfg.slotSize())
	binary.LittleEndian.PutUint32(r[offPayloadSize:], cfg.PayloadSize)
	binary.LittleEndian.PutUint32(r[offFullPolicy:], uint32(cfg.FullPolicy))
}

func (r region) storeHeaderCRC() {
	binary.LittleEndian.PutUint32(r[offHeaderCRC:], r.headerCRC())
}

func (r region) slot(index uint64, nslots, slotSize uint32) region {
	base := headerSize + uint64(index%uint64(nslots))*uint64(slotSize)
	return r[base : base+uint64(slotSize)]
}

// dataChecksum guards against torn reads (a reader racing an overwrite) and
// bit corruption. FNV-1a 64 is dependency-free and fast enough here.
func dataChecksum(b []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime64
	}
	return h
}

// OverflowError reports that one or more messages were lost before the next
// readable message, either because slow consumers were overwritten by
// producers (FullPolicyOverwrite) or because a producer crashed while holding
// a slot.
type OverflowError struct {
	// Lost is the number of messages that will never be delivered.
	Lost uint64
	// Crash is true when the gap was caused by a dead producer rather than
	// by normal overwrite pressure.
	Crash bool
}

func (e *OverflowError) Error() string {
	if e.Crash {
		return "shmipc: producer crashed, messages lost"
	}
	return "shmipc: overwritten messages detected"
}
