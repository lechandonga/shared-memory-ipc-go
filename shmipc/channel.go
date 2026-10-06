package shmipc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Options 是创建通道时的参数。附加时以共享内存头部记录为准，
// 与期望不一致会得到明确的 ErrIncompatible* 错误。
type Options struct {
	// Capacity 槽位数量（必须 > 0）。
	Capacity uint64
	// MaxMessageSize 单条消息最大字节数（默认 4096）。
	MaxMessageSize uint32
	// FullPolicy 缓冲满策略（默认覆盖最旧）。
	FullPolicy OverflowPolicy
	// Wait 空/满等待策略（默认 futex）。
	Wait WaitStrategy
	// Dir 后备文件目录（默认 /dev/shm，即 tmpfs）。
	Dir string
}

// Channel 是一个已附加到命名通道的本地句柄，可被多 goroutine 并发使用。
type Channel struct {
	name    string
	path    string
	creator bool
	file    *os.File
	data    []byte

	opts Options

	lock shmLock

	// 消费者本地的期望序号，多消费者各自独立跟踪缺口。
	recvMu     sync.Mutex
	expectSeq  uint64
	expectInit bool

	closeOnce sync.Once
}

func (o *Options) normalize() error {
	if o.Capacity == 0 {
		return errors.New("shmipc: Capacity must be > 0")
	}
	if o.MaxMessageSize == 0 {
		o.MaxMessageSize = 4096
	}
	if o.Dir == "" {
		o.Dir = "/dev/shm"
	}
	return nil
}

var errNeedWait = errors.New("shmipc: internal need wait")

// Create 创建一个新的命名通道；已存在时返回 ErrAlreadyExists。
func Create(name string, opts Options) (*Channel, error) {
	if err := opts.normalize(); err != nil {
		return nil, err
	}
	slotSize := computeSlotSize(opts.MaxMessageSize)
	size := totalSize(opts.Capacity, slotSize)

	path := channelPath(opts.Dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("shmipc: create dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrAlreadyExists, name)
		}
		return nil, fmt.Errorf("shmipc: create backing file: %w", err)
	}
	if err := f.Truncate(int64(size)); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("shmipc: size backing file: %w", err)
	}
	data, err := mmapShared(f, int(size))
	if err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}

	c := newChannel(name, path, f, data, opts, true)
	c.initHeader()
	return c, nil
}

// Attach 附加到 /dev/shm 下已存在的命名通道。
// wantCapacity/wantMaxMessageSize 大于 0 时做兼容性校验。
func Attach(name string, wantCapacity uint64, wantMaxMessageSize uint32) (*Channel, error) {
	return AttachIn("/dev/shm", name, wantCapacity, wantMaxMessageSize)
}

// AttachIn 在指定目录中附加通道。
func AttachIn(dir, name string, wantCapacity uint64, wantMaxMessageSize uint32) (*Channel, error) {
	path := channelPath(dir, name)
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotExist, name)
		}
		return nil, fmt.Errorf("shmipc: open backing file: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := int(fi.Size())
	if size < hSize {
		f.Close()
		return nil, fmt.Errorf("%w: file too small", ErrIncompatibleMagic)
	}
	data, err := mmapShared(f, size)
	if err != nil {
		f.Close()
		return nil, err
	}
	c := newChannel(name, path, f, data, Options{Dir: dir}, false)
	if err := c.verifyHeader(wantCapacity, wantMaxMessageSize); err != nil {
		c.munmap()
		f.Close()
		return nil, err
	}
	return c, nil
}

func newChannel(name, path string, f *os.File, data []byte, opts Options, creator bool) *Channel {
	c := &Channel{
		name:    name,
		path:    path,
		creator: creator,
		file:    f,
		data:    data,
		opts:    opts,
	}
	c.lock = shmLock{mem: data[offLock : offLock+lockSize]}
	return c
}

func (c *Channel) initHeader() {
	slotSize := computeSlotSize(c.opts.MaxMessageSize)
	putU64(c.data, offMagic, magic)
	putU32(c.data, offVersion, layoutVersion)
	putU32(c.data, offFlags, 0)
	putU64(c.data, offCapacity, c.opts.Capacity)
	putU32(c.data, offSlotSize, slotSize)
	putU32(c.data, offPayloadCap, c.opts.MaxMessageSize)
	putU64(c.data, offHeadSeq, 0)
	putU64(c.data, offTailSeq, 0)
	putU64(c.data, offDroppedMsgs, 0)
	putU64(c.data, offDroppedBytes, 0)
	putU64(c.data, offAbandoned, 0)
	for i := range c.data[offLock : offLock+lockSize] {
		c.data[offLock+i] = 0
	}
	putU32(c.data, offWaitRecv, 0)
	putU32(c.data, offWaitSend, 0)
	putU32(c.data, offWaitClosed, 0)
	putU32(c.data, offPolicy, uint32(c.opts.FullPolicy))
	copy(c.data[offName:hSize], []byte(c.name))
	for i := uint64(0); i < c.opts.Capacity; i++ {
		putU32(c.data, slotBase(slotSize, i)+sOffState, 0)
	}
}

func (c *Channel) verifyHeader(wantCap uint64, wantMsg uint32) error {
	if getU64(c.data, offMagic) != magic {
		return fmt.Errorf("%w: %s", ErrIncompatibleMagic, c.name)
	}
	if v := getU32(c.data, offVersion); v != layoutVersion {
		return fmt.Errorf("%w: %s (have layout %d, want %d)", ErrIncompatibleVersion, c.name, v, layoutVersion)
	}
	capacity := getU64(c.data, offCapacity)
	slotSize := getU32(c.data, offSlotSize)
	payloadCap := getU32(c.data, offPayloadCap)
	if uint64(len(c.data)) != totalSize(capacity, slotSize) {
		return fmt.Errorf("%w: file size/header mismatch", ErrIncompatibleConfig)
	}
	if wantCap != 0 && wantCap != capacity {
		return fmt.Errorf("%w: %s capacity: have %d want %d", ErrIncompatibleConfig, c.name, capacity, wantCap)
	}
	if wantMsg != 0 && wantMsg != payloadCap {
		return fmt.Errorf("%w: %s maxMessageSize: have %d want %d", ErrIncompatibleConfig, c.name, payloadCap, wantMsg)
	}
	c.opts = Options{
		Capacity:       capacity,
		MaxMessageSize: payloadCap,
		FullPolicy:     OverflowPolicy(getU32(c.data, offPolicy)),
		Wait:           WaitFutex,
		Dir:            filepath.Dir(filepath.Dir(c.path)),
	}
	return nil
}

// Name 返回通道名。
func (c *Channel) Name() string { return c.name }

// Params 返回固化在头部的容量与单消息上限。
func (c *Channel) Params() (uint64, uint32) {
	return getU64(c.data, offCapacity), getU32(c.data, offPayloadCap)
}

func (c *Channel) isClosed() bool {
	return getU32(c.data, offFlags)&flagClosed != 0
}

func (c *Channel) slotAddr(idx uint64) []byte {
	slotSize := getU32(c.data, offSlotSize)
	base := slotBase(slotSize, idx)
	return c.data[base : base+uint64(slotSize)]
}

// withLock 运行临界区；若检测到上一任持锁者崩溃并接管，
// 自动重试 fn（fn 必须只依赖共享内存中的不变量）。
func (c *Channel) withLock(deadline time.Time, fn func() error) error {
	for {
		err := c.lock.lock(deadline)
		if err != nil && !errors.Is(err, ErrLockAbandoned) {
			return err
		}
		if errors.Is(err, ErrLockAbandoned) {
			putU64(c.data, offAbandoned, getU64(c.data, offAbandoned)+1)
			// 死者可能死在“消费后/生产后、发唤醒前”，补发一次，
			// 避免外部 futex 等待者错过唯一唤醒。
			c.wakeRecv()
			c.wakeSend()
		}
		ferr := fn()
		c.lock.unlock()
		return ferr
	}
}

func timeoutDeadline(timeout time.Duration) time.Time {
	if timeout == 0 {
		return time.Time{} // 无限等待
	}
	if timeout < 0 {
		return time.Time{} // 非阻塞
	}
	return time.Now().Add(timeout)
}

func slotCrcInput(seq uint64, msg []byte) []byte {
	var buf [8]byte
	for i := 0; i < 8; i++ {
		buf[i] = byte(seq >> (8 * i))
	}
	return append(buf[:], msg...)
}
