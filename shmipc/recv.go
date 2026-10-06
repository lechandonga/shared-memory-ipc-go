package shmipc

import (
	"errors"
	"hash/crc32"
	"time"
)

// Message 是一次接收的结果。
type Message struct {
	Seq     uint64 // 全局单调提交序号
	Data    []byte // 消息体拷贝
	Skipped uint64 // 相对上一次接收错过（被覆盖）的消息条数
}

// Receive 取出最早一条未读消息。
// timeout < 0：非阻塞（空返回 ErrEmpty）；0：无限等待；>0：最长等待。
// 通道关闭时，缓冲中剩余消息仍可正常读出，排空后返回 ErrClosed。
func (c *Channel) Receive(timeout time.Duration) (Message, error) {
	deadline := timeoutDeadline(timeout)
	for {
		m, ok, err := c.tryReceive()
		if err != nil {
			return Message{}, err
		}
		if ok {
			c.wakeSend()
			return m, nil
		}
		if timeout < 0 {
			return Message{}, ErrEmpty
		}
		if werr := c.waitRecv(deadline); werr != nil {
			return Message{}, werr
		}
	}
}

func (c *Channel) tryReceive() (Message, bool, error) {
	var out Message
	var found bool
	err := c.withLock(time.Time{}, func() error {
		capacity := getU64(c.data, offCapacity)
		head := getU64(c.data, offHeadSeq)
		tail := getU64(c.data, offTailSeq)
		if head >= tail {
			if c.isClosed() {
				return ErrClosed
			}
			return errNeedWait
		}
		slot := c.slotAddr(head % capacity)
		if atomicLoadU32(slot, sOffState) != 1 {
			// 正常情况下不会发生：state=1 先于 tail 发布。
			// 若发生说明存储损坏，明确报错而非返回半条数据。
			return ErrCorrupted
		}
		seq := getU64(slot, sOffSeq)
		if seq != head {
			return ErrCorrupted
		}
		length := getU32(slot, sOffLength)
		if uint64(length) > uint64(c.opts.MaxMessageSize) {
			return ErrCorrupted
		}
		payload := make([]byte, length)
		copy(payload, slot[slotHeaderSize:slotHeaderSize+int(length)])
		if getU32(slot, sOffCRC) != crc32.ChecksumIEEE(slotCrcInput(seq, payload)) {
			return ErrCorrupted
		}

		// 消费者本地缺口跟踪：初次读取以当前序号为基线，不报告历史缺口。
		c.recvMu.Lock()
		var skipped uint64
		if c.expectInit {
			if seq > c.expectSeq {
				skipped = seq - c.expectSeq
			}
		}
		c.expectSeq = seq + 1
		c.expectInit = true
		c.recvMu.Unlock()

		putU32(slot, sOffState, 0)
		putU64(c.data, offHeadSeq, head+1)

		out = Message{Seq: seq, Data: payload, Skipped: skipped}
		found = true
		return nil
	})
	if err != nil {
		if errors.Is(err, errNeedWait) {
			return Message{}, false, nil
		}
		return Message{}, false, err
	}
	return out, found, nil
}
