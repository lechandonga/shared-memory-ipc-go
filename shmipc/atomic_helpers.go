package shmipc

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

func atomicStoreU32(b []byte, off uint64, v uint32) {
	atomic.StoreUint32((*uint32)(unsafe.Pointer(&b[off])), v)
}

func atomicLoadU32(b []byte, off uint64) uint32 {
	return atomic.LoadUint32((*uint32)(unsafe.Pointer(&b[off])))
}

func runtimeGosched() { runtime.Gosched() }
