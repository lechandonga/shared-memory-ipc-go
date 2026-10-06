package shmipc

import "errors"

var (
	// ErrClosed 通道已被销毁或处于关闭过程中。
	ErrClosed = errors.New("shmipc: channel closed")
	// ErrEmpty 当前环形缓冲中没有已提交的新消息（非阻塞接收）。
	ErrEmpty = errors.New("shmipc: channel empty")
	// ErrFull 缓冲已满且策略为非覆盖模式、又无法立即写入时返回。
	ErrFull = errors.New("shmipc: channel full")
	// ErrTimeout 在给定截止时间内未能完成收发。
	ErrTimeout = errors.New("shmipc: operation timed out")
	// ErrMessageTooLarge 消息体超过单槽容量。
	ErrMessageTooLarge = errors.New("shmipc: message larger than slot payload")
	// ErrIncompatibleMagic 附加的文件不是本通道格式（魔数不匹配）。
	ErrIncompatibleMagic = errors.New("shmipc: incompatible channel: bad magic")
	// ErrIncompatibleVersion 通道布局版本不一致，拒绝附加。
	ErrIncompatibleVersion = errors.New("shmipc: incompatible channel: layout version mismatch")
	// ErrIncompatibleConfig 容量或槽大小与创建参数不一致，拒绝附加。
	ErrIncompatibleConfig = errors.New("shmipc: incompatible channel: capacity/slotSize mismatch")
	// ErrCorrupted 槽内 CRC 校验失败：数据被覆盖到一半或底层存储损坏。
	ErrCorrupted = errors.New("shmipc: corrupted message (crc mismatch)")
	// ErrNotExist 附加的命名通道不存在。
	ErrNotExist = errors.New("shmipc: channel does not exist")
	// ErrAlreadyExists 创建时同名通道已存在。
	ErrAlreadyExists = errors.New("shmipc: channel already exists")
	// ErrLockAbandoned 上一个持锁进程在临界区中崩溃，通道已被本进程接管恢复。
	// 这是信息性结果：锁已重新可用，调用方重试此前操作即可。
	ErrLockAbandoned = errors.New("shmipc: previous lock holder crashed; state recovered")
)

// OverflowPolicy 描述缓冲满时生产者的行为。
type OverflowPolicy int

const (
	// OverflowDropOldest 缓冲满时覆盖最旧的未读消息（允许持续写入）。
	OverflowDropOldest OverflowPolicy = iota
	// OverflowBlock 缓冲满时阻塞等待，直到有消费者腾出槽位或超时/关闭。
	OverflowBlock
)

func (p OverflowPolicy) String() string {
	switch p {
	case OverflowDropOldest:
		return "DropOldest"
	case OverflowBlock:
		return "Block"
	default:
		return "Unknown"
	}
}

// WaitStrategy 描述空/满时的等待方式。
type WaitStrategy int

const (
	// WaitFutex 先短暂自旋，再通过 futex 睡眠（默认，省 CPU）。
	WaitFutex WaitStrategy = iota
	// WaitSpin 始终自旋等待（低延迟、高 CPU 占用）。
	WaitSpin
)

func (w WaitStrategy) String() string {
	switch w {
	case WaitFutex:
		return "Futex"
	case WaitSpin:
		return "Spin"
	default:
		return "Unknown"
	}
}

// Stats 是通道的瞬时状态快照。
type Stats struct {
	Capacity      uint64
	SlotSize      uint32
	HeadSeq       uint64 // 最早存活消息序号
	TailSeq       uint64 // 下一条写入消息的序号
	DroppedBytes  uint64
	DroppedMsgs   uint64
	LockAbandoned uint64
	Closed        bool
}

// Pending 返回当前可读消息条数。
func (s Stats) Pending() uint64 {
	if s.TailSeq <= s.HeadSeq {
		return 0
	}
	return s.TailSeq - s.HeadSeq
}
