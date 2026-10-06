package shmipc

import (
	"errors"
	"fmt"
	"hash/crc32"
	"time"
)

// Send 写入一条消息。
// timeout < 0：非阻塞（满且 Block 策略时返回 ErrFull）；
// timeout == 0：无限等待；> 0：最长等待时间。
// DropOldest 策略（默认）下永不阻塞：满时覆盖最旧未读消息。
func (c *Channel) Send(msg []byte, timeout time.Duration) error {
	if uint32(len(msg)) > c.opts.MaxMessageSize {
		return fmt.Errorf("%w: %d > %d", ErrMessageTooLarge, len(msg), c.opts.MaxMessageSize)
	}
	deadline := timeoutDeadline(timeout)

	for {
		var sent bool
		err := c.withLock(deadline, func() error {
			if c.isClosed() {
				return ErrClosed
			}
			capacity := getU64(c.data, offCapacity)
			head := getU64(c.data, offHeadSeq)
			tail := getU64(c.data, offTailSeq)
			if tail-head >= capacity && c.opts.FullPolicy == OverflowBlock {
				return errNeedWait
			}
			if tail-head >= capacity {
				old := c.slotAddr(head % capacity)
				oldLen := getU32(old, sOffLength)
				putU32(old, sOffState, 0)
				putU64(c.data, offHeadSeq, head+1)
				putU64(c.data, offDroppedMsgs, getU64(c.data, offDroppedMsgs)+1)
				putU64(c.data, offDroppedBytes, getU64(c.data, offDroppedBytes)+uint64(oldLen))
				head++
			}

			seq := tail
			slot := c.slotAddr(seq % capacity)
			putU32(slot, sOffState, 0)
			putU32(slot, sOffLength, uint32(len(msg)))
			copy(slot[slotHeaderSize:slotHeaderSize+len(msg)], msg)
			putU32(slot, sOffCRC, crc32.ChecksumIEEE(slotCrcInput(seq, msg)))
			putU64(slot, sOffSeq, seq)
			atomicStoreU32(slot, sOffState, 1)
			putU64(c.data, offTailSeq, seq+1)
			sent = true
			return nil
		})
		if err == nil && sent {
			c.wakeRecv()
			return nil
		}
		if errors.Is(err, errNeedWait) {
			if timeout < 0 {
				return ErrFull
			}
			if werr := c.waitSend(deadline); werr != nil {
				if c.isClosed() {
					return ErrClosed
				}
				return werr
			}
			continue
		}
		if err != nil {
			return err
		}
	}
}
