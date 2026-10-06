package itest

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// execProcess 包装子进程并收集输出，便于等待特定日志标记。
type execProcess struct {
	cmd    *exec.Cmd
	buf    bytes.Buffer
	out    chan struct{}
	closed bool
}

func wrap(cmd *exec.Cmd) *execProcess {
	p := &execProcess{cmd: cmd, out: make(chan struct{}, 8)}
	r, w, _ := os.Pipe()
	cmd.Stdout = w
	cmd.Stderr = w
	go func() {
		s := bufio.NewScanner(r)
		for s.Scan() {
			line := s.Text()
			p.buf.WriteString(line + "\n")
			log.Printf("[HELPER pid=%d] %s", cmd.Process.Pid, line)
			select {
			case p.out <- struct{}{}:
			default:
			}
		}
	}()
	return p
}

func (p *execProcess) kill(t *testing.T) {
	t.Helper()
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(os.Kill)
	}
	_ = p.cmd.Wait()
}

func (p *execProcess) mustWait(t *testing.T) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		if ee, ok := err.(*exec.ExitError); ok {
			if ee.ExitCode() != -1 && ee.String() != "signal: killed" {
				t.Fatalf("helper exited non-zero: %v\n%s", err, p.buf.String())
			}
		} else if err != nil {
			t.Fatalf("helper wait: %v", err)
		}
	case <-time.After(15 * time.Second):
		p.kill(t)
		t.Fatalf("helper timeout, output:\n%s", p.buf.String())
	}
}

func (p *execProcess) waitFor(substr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if bytes.Contains(p.buf.Bytes(), []byte(substr)) {
			return true
		}
		select {
		case <-p.out:
		case <-time.After(50 * time.Millisecond):
		}
	}
	return bytes.Contains(p.buf.Bytes(), []byte(substr))
}

func waitLog(p *execProcess, substr string, timeout time.Duration) {
	if !p.waitFor(substr, timeout) {
		// 不致命：日志经 println 输出可能丢失；调用方仍靠行为验证。
		time.Sleep(300 * time.Millisecond)
	}
}

func waitReady(t *testing.T, dir, name string, timeout time.Duration) {
	t.Helper()
	path := filepath.Join(dir, "shmipc-"+name+".shm")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f, err := os.Open(path); err == nil {
			f.Close()
			time.Sleep(200 * time.Millisecond)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("channel backing file not ready: %s", path)
}

var _ io.Writer = (*bytes.Buffer)(nil)
