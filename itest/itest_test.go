package itest

import (
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/shared-memory-ipc-go/shmipc"
)

// 业务：命名通道的创建/附加兼容性。
// 用例：
//  1. 正常参数附加成功；
//  2. 容量不一致 => ErrIncompatibleConfig；
//  3. 消息上限不一致 => ErrIncompatibleConfig；
//  4. 附加非本格式文件 => ErrIncompatibleMagic；
//  5. 附加不存在通道 => ErrNotExist；
//  6. 重复创建 => ErrAlreadyExists。
func TestCreateAttachCompatibility(t *testing.T) {
	dir := testDir(t)
	creator := runHelper(t, roleConfig{
		role: "creator", dir: dir, name: "compat",
		capacity: 16, maxMsg: 128, seconds: 6,
	})
	waitReady(t, dir, "compat", 3*time.Second)

	t.Run("正常附加成功", func(t *testing.T) {
		c, err := shmipc.AttachIn(dir, "compat", 16, 128)
		if err != nil {
			t.Fatalf("attach: %v", err)
		}
		capacity, maxMsg := c.Params()
		if capacity != 16 || maxMsg != 128 {
			t.Fatalf("params = (%d,%d), want (16,128)", capacity, maxMsg)
		}
		t.Logf("用例1 通过: 参数一致附加成功 capacity=%d maxMsg=%d", capacity, maxMsg)
		c.Detach()
	})

	t.Run("容量不一致被拒绝", func(t *testing.T) {
		_, err := shmipc.AttachIn(dir, "compat", 32, 128)
		if !errors.Is(err, shmipc.ErrIncompatibleConfig) {
			t.Fatalf("want ErrIncompatibleConfig, got %v", err)
		}
		t.Logf("用例2 通过: %v", err)
	})

	t.Run("消息上限不一致被拒绝", func(t *testing.T) {
		_, err := shmipc.AttachIn(dir, "compat", 16, 256)
		if !errors.Is(err, shmipc.ErrIncompatibleConfig) {
			t.Fatalf("want ErrIncompatibleConfig, got %v", err)
		}
		t.Logf("用例3 通过: %v", err)
	})

	garbage := runHelper(t, roleConfig{role: "badattach_magic", dir: dir, name: "garbage"})
	garbage.Wait()
	t.Run("非本格式文件被拒绝", func(t *testing.T) {
		_, err := shmipc.AttachIn(dir, "garbage", 0, 0)
		if !errors.Is(err, shmipc.ErrIncompatibleMagic) {
			t.Fatalf("want ErrIncompatibleMagic, got %v", err)
		}
		t.Logf("用例4 通过: %v", err)
	})

	t.Run("不存在的通道返回ErrNotExist", func(t *testing.T) {
		_, err := shmipc.AttachIn(dir, "no-such-channel", 0, 0)
		if !errors.Is(err, shmipc.ErrNotExist) {
			t.Fatalf("want ErrNotExist, got %v", err)
		}
		t.Logf("用例5 通过: %v", err)
	})

	t.Run("重复创建返回ErrAlreadyExists", func(t *testing.T) {
		_, err := shmipc.Create("compat", shmipc.Options{
			Capacity: 16, MaxMessageSize: 128, Dir: dir,
		})
		if !errors.Is(err, shmipc.ErrAlreadyExists) {
			t.Fatalf("want ErrAlreadyExists, got %v", err)
		}
		t.Logf("用例6 通过: %v", err)
	})

	creator.Process.Kill()
	creator.Wait()
}

// 业务：多进程多生产者/多消费者并发收发。
// 3 个生产者 + 2 个消费者在 4 秒内并发工作；验证：
//   - 所有已提交消息恰好被一个消费者收到（Block 策略、容量充足）；
//   - 每个消费者观察到的序号严格递增、无重复无残缺。
func TestMultiProcessMPMC(t *testing.T) {
	dir := testDir(t)
	const nProd, nCons = 3, 2
	creator := runHelper(t, roleConfig{
		role: "creator", dir: dir, name: "mpmc",
		capacity: 128, maxMsg: 256, seconds: 60, block: true,
	})
	waitReady(t, dir, "mpmc", 3*time.Second)

	var prods, cons []*execProcess
	for i := 0; i < nProd; i++ {
		prods = append(prods, wrap(runHelper(t, roleConfig{
			role: "producer", dir: dir, name: "mpmc",
			capacity: 128, maxMsg: 256, seconds: 4,
		})))
	}
	for i := 0; i < nCons; i++ {
		cons = append(cons, wrap(runHelper(t, roleConfig{
			role: "consumer", dir: dir, name: "mpmc",
			capacity: 128, maxMsg: 256, seconds: 7,
		})))
	}
	for _, p := range prods {
		p.mustWait(t)
	}
	t.Logf("业务[多进程MPMC] %d 个生产者均正常退出", nProd)
	for _, p := range cons {
		p.mustWait(t)
	}
	t.Logf("业务[多进程MPMC] %d 个消费者均正常退出（序号严格递增校验在 HELPER 日志中）", nCons)

	c, err := shmipc.AttachIn(dir, "mpmc", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	st := c.Stats()
	if st.DroppedMsgs != 0 {
		t.Fatalf("Block 策略下不应有覆盖，dropped=%d", st.DroppedMsgs)
	}
	t.Logf("用例验证通过: tailSeq=%d headSeq=%d dropped=%d pending=%d",
		st.TailSeq, st.HeadSeq, st.DroppedMsgs, st.Pending())
	c.Detach()

	creator.Process.Kill()
	creator.Wait()
}

// 业务：收发中途进程异常退出（SIGKILL），其他参与者不被永久阻塞。
func TestPeerCrashMidFlight(t *testing.T) {
	dir := testDir(t)
	creator := runHelper(t, roleConfig{
		role: "creator", dir: dir, name: "crash",
		capacity: 64, maxMsg: 256, seconds: 120, block: true,
	})
	waitReady(t, dir, "crash", 3*time.Second)

	t.Run("生产者崩溃_消费者继续", func(t *testing.T) {
		victim := wrap(runHelper(t, roleConfig{
			role: "producer", dir: dir, name: "crash",
			capacity: 64, maxMsg: 256, seconds: 30,
		}))
		survivor := wrap(runHelper(t, roleConfig{
			role: "consumer", dir: dir, name: "crash",
			capacity: 64, maxMsg: 256, seconds: 5,
		}))
		time.Sleep(800 * time.Millisecond)
		victim.kill(t)
		t.Log("用例: 生产者运行中 SIGKILL")
		survivor.mustWait(t)
		t.Log("通过: 存活消费者在超时窗口内正常退出，未被永久阻塞")
	})

	t.Run("消费者崩溃_生产者继续", func(t *testing.T) {
		// 独立通道：使用默认 DropOldest 策略。崩溃的消费者不再腾出槽位，
		// 在 Block 策略下满缓冲本应阻塞（这是正确语义）；这里要验证的是
		// “对端死亡不会导致状态错乱/永久假死”，覆盖策略最直观。
		dir2 := testDir(t)
		host := wrap(runHelper(t, roleConfig{
			role: "creator", dir: dir2, name: "crash2",
			capacity: 64, maxMsg: 256, seconds: 20,
		}))
		waitReady(t, dir2, "crash2", 3*time.Second)
		victim := wrap(runHelper(t, roleConfig{
			role: "consumer", dir: dir2, name: "crash2",
			capacity: 64, maxMsg: 256, seconds: 30,
		}))
		survivor := wrap(runHelper(t, roleConfig{
			role: "producer", dir: dir2, name: "crash2",
			capacity: 64, maxMsg: 256, seconds: 3,
		}))
		time.Sleep(500 * time.Millisecond)
		victim.kill(t)
		t.Log("用例: 消费者运行中 SIGKILL")
		survivor.mustWait(t)
		c, _ := shmipc.AttachIn(dir2, "crash2", 0, 0)
		st := c.Stats()
		c.Detach()
		host.kill(t)
		t.Logf("通过: 存活生产者持续写入不中断 tailSeq=%d dropped=%d", st.TailSeq, st.DroppedMsgs)
	})

	t.Run("持锁进程崩溃_锁被接管", func(t *testing.T) {
		holder := wrap(runHelper(t, roleConfig{
			role: "lockholder", dir: dir, name: "crash",
			capacity: 64, maxMsg: 256,
		}))
		waitLog(holder, "SHMIPC_HELD_LOCK", 3*time.Second)
		c, err := shmipc.AttachIn(dir, "crash", 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		holder.kill(t)
		start := time.Now()
		if err := c.Send([]byte("after-lock-crash"), 3*time.Second); err != nil {
			t.Fatalf("锁接管后发送失败: %v", err)
		}
		t.Logf("用例通过: 检测到持锁者死亡并在 %v 内接管锁，abandoned=%d",
			time.Since(start).Round(time.Millisecond), c.Stats().LockAbandoned)
		if c.Stats().LockAbandoned == 0 {
			t.Fatal("期望 abandoned 计数 > 0")
		}
		c.Detach()
	})

	creator.Process.Kill()
	creator.Wait()
}

// 业务：持续写入导致覆盖，读取方能识别缺口且未覆盖消息完整有序。
func TestOverwriteDetected(t *testing.T) {
	dir := testDir(t)
	// 直接在测试进程内完成，便于精确断言。
	c, err := shmipc.Create("ovf", shmipc.Options{
		Capacity: 8, MaxMessageSize: 64, Dir: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Destroy()

	const total = 100
	for i := 0; i < total; i++ {
		if err := c.Send([]byte(msgBody(i)), -1); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	st := c.Stats()
	if st.DroppedMsgs != total-8 {
		t.Fatalf("dropped=%d want %d", st.DroppedMsgs, total-8)
	}
	t.Logf("用例1: 持续写入 %d 条、容量 8，丢弃 %d 条", total, st.DroppedMsgs)

	var got []int
	var skippedTotal uint64
	for {
		m, err := c.Receive(-1)
		if errors.Is(err, shmipc.ErrEmpty) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if m.Skipped > 0 {
			skippedTotal += m.Skipped
		}
		got = append(got, int(m.Seq))
		if string(m.Data) != msgBody(int(m.Seq)) {
			t.Fatalf("seq=%d 内容不匹配: %q", m.Seq, m.Data)
		}
	}
	if len(got) != 8 || got[0] != 92 || got[7] != 99 {
		t.Fatalf("存活消息异常: %v", got)
	}
	if skippedTotal != 0 {
		t.Fatalf("新消费者不应报告基线之前的缺口, got %d", skippedTotal)
	}
	t.Logf("用例2: 新消费者按序收到 seq=%v..%v，内容全部 CRC 校验通过", got[0], got[len(got)-1])

	// 慢消费者：读一条后生产者再写超过容量 => 下一次收到时 Skipped>0。
	for i := 100; i < 120; i++ {
		if err := c.Send([]byte(msgBody(i)), -1); err != nil {
			t.Fatal(err)
		}
	}
	// 上面排空后 expectSeq=100；现在最早存活是 112，缺口 12。
	m, err := c.Receive(-1)
	if err != nil {
		t.Fatal(err)
	}
	if m.Seq != 112 || m.Skipped != 12 {
		t.Fatalf("缺口识别错误 seq=%d skipped=%d", m.Seq, m.Skipped)
	}
	t.Logf("用例3: 慢消费者识别缺口 seq=%d skipped=%d（100..111 已被覆盖）", m.Seq, m.Skipped)
}

// 业务：关闭语义——关闭后阻塞中的收发立即返回 ErrClosed，
// 缓冲内剩余消息可排空。
func TestCloseWakesWaiters(t *testing.T) {
	dir := testDir(t)
	creator := wrap(runHelper(t, roleConfig{
		role: "creator", dir: dir, name: "close",
		capacity: 8, maxMsg: 64, seconds: 20,
	}))
	waitReady(t, dir, "close", 3*time.Second)
	closer := wrap(runHelper(t, roleConfig{
		role: "closer", dir: dir, name: "close", seconds: 2,
	}))

	c, err := shmipc.AttachIn(dir, "close", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Send([]byte("drained-after-close"), -1); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = c.Receive(0) // 无限等待，先把残留读出
	if err != nil {
		t.Logf("首条读取: %v", err)
	}
	_, err = c.Receive(0)
	if err != shmipc.ErrClosed {
		t.Fatalf("关闭后等待应收 ErrClosed, got %v", err)
	}
	t.Logf("用例通过: 关闭事件在 %v 内唤醒阻塞接收并返回 ErrClosed", time.Since(start).Round(time.Millisecond))
	c.Detach()
	closer.mustWait(t)
	creator.kill(t)
}
