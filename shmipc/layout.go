package shmipc

import (
	"encoding/binary"
	"unsafe"
)

// 内存布局（全部小端、显式定长字段，不依赖 Go 结构体填充规则）：
//
//   [ 0 .. hSize)              通道头
//   [ hSize .. )               capacity 个定长槽
//
// 通道头字段（偏移量见 off* 常量）：
//   magic, version, flags, capacity, slotSize, payloadCap,
//   headSeq, tailSeq, droppedMsgs, droppedBytes, abandoned,
//   lock(32B), waitRecv, waitSend, waitClosed, name

const (
	layoutVersion uint32 = 1
	magic         uint64 = 0x53484d4950433031 // "SHMIPC01"

	hSize = 128

	offMagic        = 0
	offVersion      = 8
	offFlags        = 12
	offCapacity     = 16
	offSlotSize     = 24
	offPayloadCap   = 28
	offHeadSeq      = 32
	offTailSeq      = 40
	offDroppedMsgs  = 48
	offDroppedBytes = 56
	offAbandoned    = 64

	offLock       = 72
	lockSize      = 32
	offWaitRecv   = 104
	offWaitSend   = 108
	offWaitClosed = 112
	offPolicy     = 116 // uint32 OverflowPolicy
	offName       = 120 // 8 字节，截断存储，仅诊断用

	flagClosed uint32 = 1
)

// 槽布局（slotSize 字节，64B 对齐）：
//
//	state(4) length(4) crc(4) pad(4) seq(8) | payload(payloadCap)
//
// state: 0=空闲/写入中，1=已提交可读。
// crc 覆盖 seq 与 payload（length 隐含在 payload 长度中）。
const (
	slotHeaderSize = 24
	slotAlign      = 64

	sOffState  = 0
	sOffLength = 4
	sOffCRC    = 8
	sOffSeq    = 16
)

func roundUp(v, a uint32) uint32 { return (v + a - 1) / a * a }

func computeSlotSize(payloadCap uint32) uint32 {
	return roundUp(slotHeaderSize+payloadCap, slotAlign)
}

func totalSize(capacity uint64, slotSize uint32) uint64 {
	return hSize + capacity*uint64(slotSize)
}

func slotBase(slotSize uint32, idx uint64) uint64 {
	return hSize + idx*uint64(slotSize)
}

func putU64(b []byte, off uint64, v uint64) { binary.LittleEndian.PutUint64(b[off:], v) }
func putU32(b []byte, off uint64, v uint32) { binary.LittleEndian.PutUint32(b[off:], v) }
func getU64(b []byte, off uint64) uint64    { return binary.LittleEndian.Uint64(b[off:]) }
func getU32(b []byte, off uint64) uint32    { return binary.LittleEndian.Uint32(b[off:]) }

// 防御性检查：本布局按小端序列化。
var _ = func() struct{} {
	var x uint16 = 1
	if *(*byte)(unsafe.Pointer(&x)) != 1 {
		panic("shmipc: little-endian platform required")
	}
	return struct{}{}
}()
