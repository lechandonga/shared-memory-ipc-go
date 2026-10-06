package shmipc

import (
	"sync/atomic"
	"time"
	"unsafe"
)

// 等待采用“在锁外睡眠、被唤醒后重新加锁检查”的经典 futex 模式，
// 因此等待线程不会持有通道锁；持锁进程崩溃时其余等待者靠锁接管
// 与超时双重保证继续推进，不会永久阻塞。

func (c *Channel) waitWord(off uint64) *uint32 {
	return (*uint32)(unsafe.Pointer(&c.data[off]))
}

func (c *Channel) waitRecv(deadline time.Time) error {
	return c.wait(c.waitWord(offWaitRecv), deadline)
}

func (c *Channel) waitSend(deadline time.Time) error {
	return c.wait(c.waitWord(offWaitSend), deadline)
}

func (c *Channel) wait(word *uint32, deadline time.Time) error {
	if c.isClosed() {
		return ErrClosed
	}
	if c.opts.Wait == WaitSpin {
		for {
			if c.isClosed() {
				return ErrClosed
			}
			if !deadline.IsZero() && !time.Now().Before(deadline) {
				return ErrTimeout
			}
			runtimeGosched()
		}
	}
	// 每次读取当前 futex 值作为期望值；Close 与数据到达都会递增对应字。
	val := atomic.LoadUint32(word)
	if c.isClosed() {
		return ErrClosed
	}
	if err := futexWait(word, val, deadline); err != nil {
		return err
	}
	if c.isClosed() {
		return ErrClosed
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return ErrTimeout
	}
	return nil
}

func (c *Channel) wakeRecv() {
	w := c.waitWord(offWaitRecv)
	atomic.AddUint32(w, 1)
	futexWake(w, 1<<30)
}

func (c *Channel) wakeSend() {
	w := c.waitWord(offWaitSend)
	atomic.AddUint32(w, 1)
	futexWake(w, 1<<30)
}
