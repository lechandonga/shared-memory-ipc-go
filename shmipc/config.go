package shmipc

// FullPolicy selects producer behavior when the ring is full.
type FullPolicy uint32

const (
	// FullPolicyBlock makes Send wait (up to its timeout) for a free slot,
	// returning ErrFull/ErrTimeout if none becomes available. No messages are
	// ever overwritten.
	FullPolicyBlock FullPolicy = 0

	// FullPolicyOverwrite makes Send reclaim the oldest unconsumed slot once
	// it is safe to do so. Overwritten messages are reported to readers as
	// OverflowError.
	FullPolicyOverwrite FullPolicy = 1
)

const (
	// DefaultCapacity is used when Config.Capacity is zero.
	DefaultCapacity uint32 = 1024

	// DefaultPayloadSize is used when Config.PayloadSize is zero. Messages
	// larger than this are rejected with ErrMessageTooLarge.
	DefaultPayloadSize uint32 = 4096

	minCapacity    uint32 = 2
	maxCapacity    uint32 = 1 << 20
	maxPayloadSize uint32 = 1 << 22 // 4 MiB
)

// Config describes a channel's shape. It must match exactly between the
// creator and every attacher; mismatches are rejected with a specific
// incompatibility reason.
type Config struct {
	// Capacity is the number of ring slots (maximum in-flight messages).
	Capacity uint32

	// PayloadSize is the usable per-slot payload in bytes. It is rounded up
	// to a multiple of 16 for alignment; PayloadSize as reported by Config
	// stays the requested value and slots store that many usable bytes.
	PayloadSize uint32

	// FullPolicy selects blocking vs. overwrite behavior for producers.
	FullPolicy FullPolicy
}

func (c Config) slotSize() uint32 {
	total := slotHdrSize + c.PayloadSize
	return (total + 15) &^ 15
}

func (c *Config) normalize() error {
	if c.Capacity == 0 {
		c.Capacity = DefaultCapacity
	}
	if c.PayloadSize == 0 {
		c.PayloadSize = DefaultPayloadSize
	}
	if c.Capacity < minCapacity || c.Capacity > maxCapacity {
		return ErrInvalidConfig
	}
	if c.PayloadSize > maxPayloadSize {
		return ErrInvalidConfig
	}
	if c.FullPolicy != FullPolicyBlock && c.FullPolicy != FullPolicyOverwrite {
		return ErrInvalidConfig
	}
	return nil
}

func (c Config) totalSize() uint64 {
	return uint64(headerSize) + uint64(c.Capacity)*uint64(c.slotSize())
}

// MaxMessageSize returns the largest message accepted by Send.
func (c Config) MaxMessageSize() int { return int(c.PayloadSize) }
