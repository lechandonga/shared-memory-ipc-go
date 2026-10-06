//go:build linux

package shmipc

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// mmapShared 对后备文件做 MAP_SHARED 映射。放在 /dev/shm 时后备为 tmpfs，
// 数据只驻留内存；放在普通目录时行为一致，只是内存压力下可换出到磁盘。
// 所有进程映射同一文件偏移即指向同一物理页，配合 futex 实现跨进程同步。
func mmapShared(f *os.File, size int) ([]byte, error) {
	addr, errno := syscall.Mmap(int(f.Fd()), 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if errno != nil {
		return nil, fmt.Errorf("shmipc: mmap: %w", errno)
	}
	return addr, nil
}

func munmapBytes(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return syscall.Munmap(b)
}

func (c *Channel) munmap() error {
	if c.data == nil {
		return nil
	}
	err := munmapBytes(c.data)
	c.data = nil
	return err
}

var _ = unsafe.Sizeof(0)
