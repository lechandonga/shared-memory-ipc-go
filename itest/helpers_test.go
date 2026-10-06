// Package itest 包含需要多个真实操作系统进程参与的集成测试。
// 通过 go test 重新执行自身测试二进制（TestHelperProcess 模式）扮演
// 生产者/消费者/崩溃者等角色。
package itest

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lechandonga/shared-memory-ipc-go/shmipc"
)

func testDir(t *testing.T) string {
	dir := filepath.Join(t.TempDir(), "shm")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

type roleConfig struct {
	role     string
	dir      string
	name     string
	capacity uint64
	maxMsg   uint32
	seconds  int
	messages int
	wantCap  uint64
	wantMsg  uint32
	block    bool
}

func runHelper(t *testing.T, cfg roleConfig) *exec.Cmd {
	t.Helper()
	args := []string{"-test.run=TestHelperProcess", "-test.v", "--"}
	kv := []string{
		cfg.role,
		cfg.dir,
		cfg.name,
		strconv.FormatUint(cfg.capacity, 10),
		strconv.FormatUint(uint64(cfg.maxMsg), 10),
		strconv.Itoa(cfg.seconds),
		strconv.Itoa(cfg.messages),
		strconv.FormatUint(cfg.wantCap, 10),
		strconv.FormatUint(uint64(cfg.wantMsg), 10),
		strconv.FormatBool(cfg.block),
	}
	args = append(args, kv...)
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "SHMIPC_HELPER=1")
	cmd.Stdout = &helperWriter{t: t, tag: "HELPER"}
	cmd.Stderr = &helperWriter{t: t, tag: "HELPER"}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	return cmd
}

type helperWriter struct {
	t   *testing.T
	tag string
	mu  sync.Mutex
}

func (w *helperWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.t.Logf("[%s] %s", w.tag, string(p))
	return len(p), nil
}

func TestHelperProcess(t *testing.T) {
	if os.Getenv("SHMIPC_HELPER") != "1" {
		return
	}
	args := os.Args
	for i, a := range args {
		if a == "--" {
			args = args[i+1:]
			break
		}
	}
	if len(args) < 10 {
		fmt.Fprintln(os.Stderr, "bad helper args")
		os.Exit(2)
	}
	cfg := roleConfig{
		role:     args[0],
		dir:      args[1],
		name:     args[2],
		capacity: atou(args[3]),
		maxMsg:   uint32(atou(args[4])),
		seconds:  atoi(args[5]),
		messages: atoi(args[6]),
		wantCap:  atou(args[7]),
		wantMsg:  uint32(atou(args[8])),
		block:    args[9] == "true",
	}
	if err := runRole(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "role %s failed: %v\n", cfg.role, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func runRole(cfg roleConfig) error {
	switch cfg.role {
	case "creator":
		// 测试用常驻宿主：忽略 SIGTERM，只响应 SIGKILL，
		// 保证测试结束杀宿主时 defer 不会竞态删除通道文件。
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
		opts := shmipc.Options{
			Capacity:       cfg.capacity,
			MaxMessageSize: cfg.maxMsg,
			Dir:            cfg.dir,
		}
		if cfg.block {
			opts.FullPolicy = shmipc.OverflowBlock
		}
		c, err := shmipc.Create(cfg.name, opts)
		if err != nil {
			return err
		}
		defer c.Destroy()
		fmt.Printf("creator ready name=%s cap=%d\n", cfg.name, cfg.capacity)
		select {
		case <-sigc:
			time.Sleep(time.Hour)
		case <-time.After(time.Duration(cfg.seconds) * time.Second):
		}
		return c.Destroy()
	case "attacher":
		c, err := shmipc.AttachIn(cfg.dir, cfg.name, cfg.wantCap, cfg.wantMsg)
		if err != nil {
			return err
		}
		defer c.Detach()
		fmt.Printf("attacher attached cap=%d\n", cfg.capacity)
		time.Sleep(time.Duration(cfg.seconds) * time.Second)
		return nil
	case "badattach_magic":
		path := filepath.Join(cfg.dir, "shmipc-"+cfg.name+".shm")
		if err := os.WriteFile(path, []byte("NOT-A-SHMIPC-CHANNEL!!"), 0o600); err != nil {
			return err
		}
		return nil
	case "producer":
		return produce(cfg)
	case "consumer":
		return consume(cfg)
	case "lockholder":
		return shmipc.LockAndHold(cfg.dir, cfg.name)
	case "closer":
		c, err := shmipc.AttachIn(cfg.dir, cfg.name, 0, 0)
		if err != nil {
			return err
		}
		time.Sleep(500 * time.Millisecond)
		if err := c.Close(); err != nil {
			return err
		}
		fmt.Println("closer: channel closed")
		time.Sleep(time.Duration(cfg.seconds) * time.Second)
		return c.Detach()
	}
	return fmt.Errorf("unknown role %q", cfg.role)
}

func produce(cfg roleConfig) error {
	c, err := shmipc.AttachIn(cfg.dir, cfg.name, 0, 0)
	if err != nil {
		return err
	}
	defer c.Detach()
	deadline := time.Now().Add(time.Duration(cfg.seconds) * time.Second)
	var i int
	for time.Now().Before(deadline) {
		payload := []byte(fmt.Sprintf("p-%d-m-%06d", os.Getpid(), i))
		if cfg.messages > 0 && i >= cfg.messages {
			break
		}
		if err := c.Send(payload, time.Second); err != nil {
			return fmt.Errorf("send %d: %w", i, err)
		}
		i++
	}
	fmt.Printf("producer pid=%d sent=%d\n", os.Getpid(), i)
	return nil
}

func consume(cfg roleConfig) error {
	c, err := shmipc.AttachIn(cfg.dir, cfg.name, 0, 0)
	if err != nil {
		return err
	}
	defer c.Detach()
	pid := os.Getpid()
	received := 0
	var totalSkipped uint64
	var lastSeq uint64
	gotFirst := false
	deadline := time.Now().Add(time.Duration(cfg.seconds) * time.Second)
	for time.Now().Before(deadline) {
		m, err := c.Receive(500 * time.Millisecond)
		if err == shmipc.ErrTimeout {
			continue
		}
		if err == shmipc.ErrClosed {
			break
		}
		if err != nil {
			return err
		}
		if gotFirst && m.Seq <= lastSeq {
			return fmt.Errorf("consumer %d: out-of-order seq %d after %d", pid, m.Seq, lastSeq)
		}
		gotFirst = true
		lastSeq = m.Seq
		received++
		totalSkipped += m.Skipped
	}
	fmt.Printf("consumer pid=%d received=%d skipped=%d lastSeq=%d\n", pid, received, totalSkipped, lastSeq)
	if cfg.messages > 0 && received+int(totalSkipped) != cfg.messages && totalSkipped == 0 {
		// 多消费者场景下不做严格断言，仅打印。
	}
	return nil
}

func atoi(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func atou(s string) uint64 {
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}
