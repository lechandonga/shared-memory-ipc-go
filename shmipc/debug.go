package shmipc

import (
	"time"
)

// LockAndHold 仅供测试/故障注入使用：附加通道后获取内部锁并保持，
// 不响应任何条件，直到调用进程被杀死，用于模拟“持锁者在临界区崩溃”。
// 其他进程会通过属主存活检测接管锁并累加 Stats.LockAbandoned。
func LockAndHold(dir, name string) error {
	c, err := AttachIn(dir, name, 0, 0)
	if err != nil {
		return err
	}
	if err := c.lock.lock(time.Time{}); err != nil && err != ErrLockAbandoned {
		return err
	}
	// 阻塞直到被外部杀死。使用真实的定时器通道而非空 select，
	// 避免 Go 测试运行时的 "all goroutines asleep" 死锁检测误报。
	<-time.After(time.Hour)
	return nil
}
