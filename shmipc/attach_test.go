package shmipc_test

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/lechandonga/shared-memory-ipc-go/shmipc"
)

// Unit-level coverage for the attach protocol, naming, size limits and the
// empty/closed/too-large result distinctions. These complement the
// multi-process business tests.
func TestAttachProtocolRejections(t *testing.T) {
	log := newBusinessLogger(t)
	log.Business("attach protocol and result distinctions")

	cfg := shmipc.Config{Capacity: 16, PayloadSize: 64}
	name := "unit-attach"
	ch, norm, err := shmipc.Create(name, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Destroy()

	log.Case("create duplicate rejected")
	if _, _, err := shmipc.Create(name, cfg); !errors.Is(err, shmipc.ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}

	log.Case("attach missing name rejected with ErrNotExist")
	if _, err := shmipc.Attach("no-such-channel-xyz", norm); !errors.Is(err, shmipc.ErrNotExist) {
		t.Fatalf("want ErrNotExist, got %v", err)
	}

	log.Case("invalid names rejected")
	for _, bad := range []string{"", "../escape", "a/b", "name with space", "x=y"} {
		if _, _, err := shmipc.Create(bad, norm); !errors.Is(err, shmipc.ErrInvalidName) {
			t.Fatalf("name %q: want ErrInvalidName, got %v", bad, err)
		}
	}

	log.Case("magic mismatch on foreign shm file")
	foreign := "/dev/shm/shmipc-foreign"
	buf := make([]byte, 4096*64) // large enough to pass the size check
	// zero content => magic won't match
	if err := os.WriteFile(foreign, buf, 0o600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(foreign)
	if _, err := shmipc.Attach("foreign", norm); !errors.Is(err, shmipc.ReasonBadMagic) {
		t.Fatalf("want ReasonBadMagic, got %v", err)
	}

	log.Case("oversized message rejected before touching ring")
	if err := ch.Send(make([]byte, norm.PayloadSize+1), 0); !errors.Is(err, shmipc.ErrMessageTooLarge) {
		t.Fatalf("want ErrMessageTooLarge, got %v", err)
	}

	log.Case("empty then closed distinctions")
	if _, _, err := ch.Recv(0); !errors.Is(err, shmipc.ErrEmpty) {
		t.Fatalf("want ErrEmpty, got %v", err)
	}
	if err := ch.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ch.Recv(-1); !errors.Is(err, shmipc.ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
	if err := ch.Send([]byte("x"), 0); !errors.Is(err, shmipc.ErrClosed) {
		t.Fatalf("want ErrClosed send, got %v", err)
	}
	log.Pass("all attach/result distinctions")

	_ = time.Millisecond
}
