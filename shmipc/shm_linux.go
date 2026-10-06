//go:build linux && (amd64 || arm64)

package shmipc

import (
	"errors"
	"os"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	shmDir = "/dev/shm"

	protRead  = syscall.PROT_READ
	protWrite = syscall.PROT_WRITE
	mapShared = syscall.MAP_SHARED
)

func shmPath(name string) string { return shmDir + "/shmipc-" + name }

// createFile creates the backing file exclusively and sizes it.
func createFile(path string, size int) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return nil, ErrAlreadyExists
		}
		return nil, err
	}
	if err := f.Truncate(int64(size)); err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}
	return f, nil
}

func openFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotExist
		}
		return nil, err
	}
	return f, nil
}

func mapRegion(f *os.File, size int) (region, error) {
	b, err := syscall.Mmap(int(f.Fd()), 0, size, protRead|protWrite, mapShared)
	if err != nil {
		return nil, err
	}
	return region(b), nil
}

func unmapRegion(r region) error { return syscall.Munmap([]byte(r)) }

// processAlive reports whether pid currently exists. kill(pid, 0) fails with
// ESRCH for dead PIDs and EPERM for alive-but-foreign ones; both are
// distinguishable from each other, but for our purposes EPERM means "alive".
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// futexWait32 blocks while *addr == value, or until dur elapses. spurious
// wakeups are expected and handled by callers re-checking their predicates.
func futexWait32(addr *uint32, value uint32, dur time.Duration) {
	var ts syscall.Timespec
	if dur > 0 {
		ns := dur.Nanoseconds()
		ts.Sec = ns / 1e9
		ts.Nsec = ns % 1e9
	}
	for {
		_, _, errno := syscall.Syscall6(
			sysFutex,
			uintptr(unsafe.Pointer(addr)),
			uintptr(futexWait),
			uintptr(value),
			uintptr(unsafe.Pointer(&ts)),
			0, 0,
		)
		if errno != syscall.EINTR {
			return
		}
	}
}

func futexWake32(addr *uint32, n int) {
	syscall.Syscall6(sysFutex, uintptr(unsafe.Pointer(addr)),
		uintptr(futexWake), uintptr(n), 0, 0, 0)
}

func loadClosed(r region) bool { return atomic.LoadUint32(r.u32(offClosed)) != 0 }
