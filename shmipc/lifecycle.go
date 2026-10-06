package shmipc

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
	"unsafe"
)

// Close 标记通道关闭并唤醒所有等待者。已缓冲的消息仍可被接收方排空，
// 排空后 Receive 返回 ErrClosed。Close 幂等。
func (c *Channel) Close() error {
	var already bool
	c.closeOnce.Do(func() {
		atomic.OrUint32((*uint32)(unsafe.Pointer(&c.data[offFlags])), flagClosed)
		c.wakeRecv()
		c.wakeSend()
		w := c.waitWord(offWaitClosed)
		atomic.AddUint32(w, 1)
		futexWake(w, 1<<30)
	})
	_ = already
	return nil
}

// Destroy 关闭通道、解除映射并删除后备文件。
// 只有创建者（或主动接管的运维方）应调用；并发附加者随后收到 ErrClosed
// 或 ErrNotExist。未关闭映射的进程仍可继续访问已映射内存直到自身退出。
func (c *Channel) Destroy() error {
	c.Close()
	var firstErr error
	if err := c.munmap(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := c.file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := os.Remove(c.path); err != nil && !os.IsNotExist(err) && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Detach 仅解除本进程的映射，不删除通道文件（普通参与者退出时调用）。
// 注意：Detach 不会关闭共享通道——任何参与者正常退出都不影响其他人，
// 这是崩溃安全要求的一部分；全局关闭只能显式调用 Close/Destroy。
func (c *Channel) Detach() error {
	err1 := c.munmap()
	err2 := c.file.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// Stats 返回通道瞬时状态快照。
func (c *Channel) Stats() Stats {
	return Stats{
		Capacity:      getU64(c.data, offCapacity),
		SlotSize:      getU32(c.data, offSlotSize),
		HeadSeq:       getU64(c.data, offHeadSeq),
		TailSeq:       getU64(c.data, offTailSeq),
		DroppedBytes:  getU64(c.data, offDroppedBytes),
		DroppedMsgs:   getU64(c.data, offDroppedMsgs),
		LockAbandoned: getU64(c.data, offAbandoned),
		Closed:        c.isClosed(),
	}
}

// WaitClosed 阻塞直到通道被任意参与者关闭，或超时（timeout>0）。
func (c *Channel) WaitClosed(timeout time.Duration) error {
	word := c.waitWord(offWaitClosed)
	deadline := timeoutDeadline(timeout)
	for {
		if c.isClosed() {
			return nil
		}
		val := atomic.LoadUint32(word)
		if err := futexWait(word, val, deadline); err != nil {
			return err
		}
	}
}

func channelPath(dir, name string) string {
	// 通道名只允许路径安全字符，避免越出后备目录。
	safe := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '.', r == '-', r == '_':
			safe = append(safe, r)
		default:
			safe = append(safe, '_')
		}
	}
	return filepath.Join(dir, "shmipc-"+string(safe)+".shm")
}
