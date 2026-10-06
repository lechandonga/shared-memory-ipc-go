//go:build linux

package shmipc

import (
	"syscall"
	"time"
	"unsafe"
)

// 直接使用 SYS_FUTEX 系统调用，不引入第三方依赖。
// futex 字必须是共享映射中的 4 字节对齐地址；MAP_SHARED 文件映射
// 在所有进程间共享同一物理页，因此 FUTEX_WAKE 可跨进程唤醒。

const (
	futexWaitShared = 0
	futexWakeShared = 1
)

func futexWait(addr *uint32, expect uint32, deadline time.Time) error {
	var ts syscall.Timespec
	var tsp unsafe.Pointer
	if !deadline.IsZero() {
		d := time.Until(deadline)
		if d <= 0 {
			return ErrTimeout
		}
		ts = syscall.NsecToTimespec(d.Nanoseconds())
		tsp = unsafe.Pointer(&ts)
	}
	// FUTEX_WAIT = 0（共享语义，无 PRIVATE 标志）。
	for {
		_, _, errno := syscall.Syscall6(
			syscall.SYS_FUTEX,
			uintptr(unsafe.Pointer(addr)),
			uintptr(futexWaitShared),
			uintptr(expect),
			uintptr(tsp),
			0, 0,
		)
		if errno == 0 {
			return nil
		}
		switch errno {
		case syscall.EAGAIN:
			// *addr != expect：值已变化，调用方应重新检查条件。
			return nil
		case syscall.EINTR:
			continue
		case syscall.ETIMEDOUT:
			return ErrTimeout
		default:
			return errno
		}
	}
}

func futexWake(addr *uint32, n int32) {
	for {
		_, _, errno := syscall.Syscall6(
			syscall.SYS_FUTEX,
			uintptr(unsafe.Pointer(addr)),
			uintptr(futexWakeShared),
			uintptr(n),
			0, 0, 0,
		)
		if errno != syscall.EINTR {
			return
		}
	}
}

// atomicPointer 把共享内存中的 4 字节对齐切片转换为 atomic/futex 可用的 *uint32。
func atomicPointer(b []byte) unsafe.Pointer {
	return unsafe.Pointer(&b[0])
}
