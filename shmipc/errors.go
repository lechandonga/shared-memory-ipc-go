// Package shmipc implements a local, POSIX shared-memory based,
// multi-producer/multi-consumer (MPMC) inter-process message channel.
//
// All processes participating in a channel must run on the same machine and
// must be Linux/amd64 or Linux/arm64 binaries. Producers never block past the
// configured timeout (or not at all, depending on FullPolicy): when consumers
// fall behind, not-yet-consumed messages may be overwritten and consumers are
// informed of the loss explicitly through [OverflowError] instead of silently
// receiving torn or stale data.
package shmipc

import "errors"

// Distinguishable, documented error conditions. All timeouts/deadlines also
// wrap these sentinels so callers can use errors.Is.
var (
	// ErrEmpty is returned by non-blocking receives when no readable message
	// is currently available.
	ErrEmpty = errors.New("shmipc: channel empty")

	// ErrFull is returned by sends with FullPolicyReturn when every slot is
	// occupied by unconsumed messages.
	ErrFull = errors.New("shmipc: channel full")

	// ErrTimeout is returned when a message could not be sent/received before
	// the configured deadline.
	ErrTimeout = errors.New("shmipc: operation timed out")

	// ErrClosed is returned by operations on a channel that has been closed
	// by a participant or destroyed.
	ErrClosed = errors.New("shmipc: channel closed")

	// ErrMessageTooLarge is returned when a message exceeds MaxMessageSize
	// (derived from the channel's payload capacity).
	ErrMessageTooLarge = errors.New("shmipc: message larger than slot payload")

	// ErrIncompatible is returned by Attach when an existing channel exists
	// but its parameters or binary layout do not match what the caller expects.
	// It always wraps a more specific reason.
	ErrIncompatible = errors.New("shmipc: incompatible channel")

	// ErrNotExist is returned by Attach when no channel with the given name
	// currently exists.
	ErrNotExist = errors.New("shmipc: channel does not exist")

	// ErrAlreadyExists is returned by Create when the name is already taken.
	ErrAlreadyExists = errors.New("shmipc: channel already exists")

	// ErrInvalidName is returned when a channel name is unsafe for use as a
	// file name under /dev/shm.
	ErrInvalidName = errors.New("shmipc: invalid channel name")

	// ErrInvalidConfig is returned when configuration parameters are out of
	// range.
	ErrInvalidConfig = errors.New("shmipc: invalid configuration")
)

// Incompatibility reasons, each wraps [ErrIncompatible] and carries a human
// readable message. Callers distinguish them with errors.Is.
var (
	ReasonBadMagic         = incompatError{"magic signature mismatch (not a shmipc channel)"}
	ReasonBadVersion       = incompatError{"incompatible binary/ABI version"}
	ReasonCapacityMismatch = incompatError{"slot capacity differs from requested configuration"}
	ReasonPayloadMismatch  = incompatError{"payload size differs from requested configuration"}
	ReasonSizeMismatch     = incompatError{"total mapped size differs from requested configuration"}
	ReasonTruncated        = incompatError{"existing backing file is smaller than expected"}
	ReasonCorruptHeader    = incompatError{"header checksum mismatch (memory corruption)"}
)

type incompatError struct{ msg string }

func (e incompatError) Error() string { return e.msg + ": " + ErrIncompatible.Error() }
func (e incompatError) Unwrap() error { return ErrIncompatible }
