package shmipc

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// 锁在通道头内占 32 字节：
//   +0  uint32 word       0=空闲 1=已锁（同时作为 futex 字）
//   +4  int32  ownerPid   持锁进程 PID
//   +8  uint64 ownerStart 持锁进程 starttime（/proc/<pid>/stat 第 22 列，clock ticks）
//   +16 uint64 保留
//
// 崩溃恢复规则：竞争等待者周期性检查 ownerPid 是否仍存活（进程存在且
// starttime 一致）。若 owner 已死亡（被杀/OOM/掉电以外的异常退出），
// 等待者直接接管锁、累加 abandoned 计数并唤醒其他等待者。owner 只是被
// 抢占（仍然存活）时永远不会被抢占锁，因此不会破坏临界区状态。

const (
	lwWord   = 0
	lwPid    = 4
	lwStart  = 8
	spinIter = 64
)

type shmLock struct {
	mem []byte // 指向 header 中 offLock 处的 32 字节
}

func (l *shmLock) wordAddr() *uint32 {
	// offLock=72 为 4 字节对齐，可安全作为 atomic/futex 字。
	return (*uint32)(atomicPointer(l.mem[lwWord:]))
}

// lock 在 deadline 前获取锁。返回 ErrLockAbandoned 表示检测到上一任持锁
// 进程崩溃、锁已被本次调用接管（调用方应重试基于不变量的操作）。
func (l *shmLock) lock(deadline time.Time) error {
	myPid := os.Getpid()
	myStart := procStarttime(myPid)

	spins := 0
	for {
		if atomic.CompareAndSwapUint32(l.wordAddr(), 0, 1) {
			putS32(l.mem, lwPid, int32(myPid))
			putU64(l.mem, lwStart, myStart)
			return nil
		}
		// 短暂自旋
		for i := 0; i < spinIter; i++ {
			runtime.Gosched()
			if atomic.CompareAndSwapUint32(l.wordAddr(), 0, 1) {
				putS32(l.mem, lwPid, int32(myPid))
				putU64(l.mem, lwStart, myStart)
				return nil
			}
		}
		spins++

		now := time.Now()
		if !deadline.IsZero() && !now.Before(deadline) {
			return ErrTimeout
		}
		// futex 最多睡 5ms：必须周期性醒来检查持锁者是否已崩溃，
		// 否则被杀的持锁者永远不会执行 unlock/wake，等待者将永久睡眠。
		// 醒来后立即做存活检查（前 10 轮先给活锁一点时间，避免误读 owner 字段）。
		wakeDeadline := now.Add(5 * time.Millisecond)
		if !deadline.IsZero() && deadline.Before(wakeDeadline) {
			wakeDeadline = deadline
		}
		if err := futexWait(l.wordAddr(), 1, wakeDeadline); err == ErrTimeout &&
			!deadline.IsZero() && !time.Now().Before(deadline) {
			return ErrTimeout
		}
		if spins > 10 && l.ownerDead() {
			// 接管崩溃 owner 的锁。
			atomic.StoreUint32(l.wordAddr(), 1)
			putS32(l.mem, lwPid, int32(myPid))
			putU64(l.mem, lwStart, myStart)
			// 接管发生时，我们正持有“逻辑上属于死者”的锁：直接交还给调用者，
			// 由调用方在临界区内依据共享内存不变量重新判定/推进。
			futexWake(l.wordAddr(), 1<<30)
			return ErrLockAbandoned
		}
	}
}

func (l *shmLock) unlock() {
	putS32(l.mem, lwPid, 0)
	putU64(l.mem, lwStart, 0)
	atomic.StoreUint32(l.wordAddr(), 0)
	futexWake(l.wordAddr(), 1)
}

func (l *shmLock) ownerDead() bool {
	pid := int(getS32(l.mem, lwPid))
	if pid <= 0 {
		// 没有 owner 记录：保守认为未死亡，交由 word 状态决定。
		return false
	}
	start := getU64(l.mem, lwStart)
	aliveStart, ok := tryProcStarttime(pid)
	if !ok {
		return true // /proc 中已无此进程
	}
	return aliveStart != start // PID 被复用：starttime 不一致
}

func putS32(b []byte, off int, v int32) {
	putU32(b, uint64(off), uint32(v))
}

func getS32(b []byte, off int) int32 {
	return int32(getU32(b, uint64(off)))
}

func procStarttime(pid int) uint64 {
	if s, ok := tryProcStarttime(pid); ok {
		return s
	}
	return 0
}

// tryProcStarttime 读取 /proc/<pid>/stat 第 22 个字段（starttime, clock ticks）。
func tryProcStarttime(pid int) (uint64, bool) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, false
	}
	s := string(data)
	// comm 字段可能含空格与括号，取最后一个 ')' 之后的部分。
	rp := strings.LastIndexByte(s, ')')
	if rp < 0 || rp+2 >= len(s) {
		return 0, false
	}
	fields := strings.Fields(s[rp+2:])
	// ')' 之后第一个字段是 state（第 3 列），starttime 是第 22 列 => 索引 19。
	const startIdx = 22 - 3
	if len(fields) <= startIdx {
		return 0, false
	}
	v, err := strconv.ParseUint(fields[startIdx], 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
