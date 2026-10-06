package shmipc_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// helperProcess builds tests/helper once and returns a configured command.
type helper struct {
	bin string
}

var (
	helperOnce sync.Once
	helperBin  string
	helperErr  error
)

func buildHelper(t *testing.T) string {
	t.Helper()
	helperOnce.Do(func() {
		dir := os.TempDir()
		bin := filepath.Join(dir, "shmipc-testhelper")
		cmd := exec.Command("go", "build", "-o", bin, "./tests/helper")
		cmd.Dir = ".."
		var out bytes.Buffer
		cmd.Stderr = &out
		if err := cmd.Run(); err != nil {
			helperErr = fmt.Errorf("build helper: %w: %s", err, out.String())
			return
		}
		helperBin = bin
	})
	if helperErr != nil {
		t.Fatal(helperErr)
	}
	return helperBin
}

type proc struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *syncBuffer
	stderr *syncBuffer
	name   string
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}
func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func startHelper(t *testing.T, bin string, args []string, env []string) *proc {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), env...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out := &syncBuffer{}
	errc := &syncBuffer{}
	cmd.Stdout = out
	cmd.Stderr = errc
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{cmd: cmd, stdin: stdin, stdout: out, stderr: errc}
	p.name = fmt.Sprint(args[:6])
	return p
}

func (p *proc) wait(t *testing.T) error {
	t.Helper()
	p.stdin.Close()
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Fatalf("helper %v hung; stdout=%s stderr=%s", p.name, p.stdout.String(), p.stderr.String())
		return nil
	}
}

func (p *proc) kill() { _ = p.cmd.Process.Kill() }

func commonArgs(name string, cfg testCfg, mode string, extra ...string) []string {
	args := []string{
		"-name", name,
		"-cap", strconv.Itoa(int(cfg.capacity)),
		"-payload", strconv.Itoa(int(cfg.payload)),
		"-mode", mode,
	}
	args = append(args, extra...)
	return args
}

type testCfg struct {
	capacity uint32
	payload  uint32
}
