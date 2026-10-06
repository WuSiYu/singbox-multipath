package multipath

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// Protocol-level regression fixture, not a QUIC or CPU benchmark. Each reliable
// FIFO link has a fixed wire rate, one-way propagation delay and bounded writes.
// Like a child socket whose send buffer autotunes to about twice its window,
// a Write blocks while more than one round trip of data waits to depart.
// synctest advances network time deterministically without privileged netem.
type pacedPacket struct {
	data []byte
	at   time.Time
}
type pacedLink struct {
	net.Conn
	rate      int64
	delay     time.Duration
	packets   chan pacedPacket
	stopped   chan struct{}
	closeOnce sync.Once
	next      time.Time // Written only by the single per-leg writer.
	backlog   time.Duration
}

func newPacedLink(conn net.Conn, rate int64, delay time.Duration) *pacedLink {
	c := &pacedLink{Conn: conn, rate: rate, delay: delay, packets: make(chan pacedPacket, 256), stopped: make(chan struct{})}
	c.backlog = max(2*delay, time.Duration(2*65536*int64(time.Second)/rate))
	go func() {
		for {
			select {
			case <-c.stopped:
				return
			case packet := <-c.packets:
				timer := time.NewTimer(max(0, time.Until(packet.at)))
				select {
				case <-c.stopped:
					timer.Stop()
					return
				case <-timer.C:
				}
				if err := writeAll(c.Conn, packet.data); err != nil {
					c.Close()
					return
				}
			}
		}
	}()
	return c
}

func (c *pacedLink) Write(p []byte) (int, error) {
	if wait := time.Until(c.next) - c.backlog; !c.next.IsZero() && wait > 0 {
		select {
		case <-c.stopped:
			return 0, net.ErrClosed
		case <-time.After(wait):
		}
	}
	now := time.Now()
	if now.After(c.next) {
		c.next = now
	}
	c.next = c.next.Add(time.Duration(int64(len(p)) * int64(time.Second) / c.rate))
	packet := pacedPacket{data: append([]byte(nil), p...), at: c.next.Add(c.delay)}
	select {
	case <-c.stopped:
		return 0, net.ErrClosed
	case c.packets <- packet:
		return len(p), nil
	}
}
func (c *pacedLink) Close() error { c.closeOnce.Do(func() { close(c.stopped) }); return c.Conn.Close() }

func TestPerformanceHealthyLinks(t *testing.T) {
	// Baselines are this version's own on this fixture. Earlier baselines
	// (beta3) came from links that accepted 16 MiB per flow without blocking,
	// up to seconds of queue that no child transport builds: a scheduler that
	// keeps every path busy, as MPTCP does, was punished for it.
	// Allow 2% for goroutine scheduling / frame-assignment variation, not a lost RTT.
	for _, tc := range []struct {
		parallel, rate0, rate1, rtt0, rtt1 int
		baselineMbps                       float64
	}{
		{1, 160, 600, 65, 110, 725.1}, {8, 160, 600, 65, 110, 730.0}, {32, 160, 600, 65, 110, 744.6},
		{1, 40, 50, 65, 110, 89.6}, {8, 40, 50, 65, 110, 89.7},
		{1, 160, 600, 65, 400, 670.6}, {8, 160, 600, 65, 400, 667.1},
		{1, 160, 600, 110, 65, 716.0}, {1, 160, 600, 5, 10, 756.3}, {8, 160, 600, 5, 10, 756.8},
		{1, 1000, 1000, 65, 110, 1781.5},
	} {
		parallel := tc.parallel
		t.Run(fmt.Sprintf("%d_%d+%dMbps_%d+%dms", parallel, tc.rate0, tc.rate1, tc.rtt0, tc.rtt1), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := testCoreConfig()
				cfg.FrameSize = 65536
				cfg.QueueFrames = 256
				cfg.QueueBytes = 16 << 20
				// The old 2048-frame ceiling was above this fixture's byte cap.
				cfg.ReceiveWindowBytes = 64 << 20
				cfg.SendBufferBytes = 64 << 20
				cfg.PathStallTimeoutMin = time.Second
				budget := int64(512 << 20)
				senderMemory, receiverMemory := newMemoryBudget(budget, false), newMemoryBudget(budget, false)
				var senders, receivers []*mpCore
				var sources, targets []net.Conn
				for i := 0; i < parallel; i++ {
					cfg.Memory = senderMemory
					left, app := newCore(context.Background(), cfg)
					cfg.Memory = receiverMemory
					right, peer := newCore(context.Background(), cfg)
					senders = append(senders, left)
					receivers = append(receivers, right)
					sources = append(sources, app)
					targets = append(targets, peer)
					left.activate(activationInfo{Reason: activationReasonBytes})
					right.activate(activationInfo{Reason: activationReasonBytes})
					for id := uint8(0); id < 2; id++ {
						a, b := net.Pipe()
						rate, delay := int64(tc.rate0*1_000_000/8/parallel), time.Duration(tc.rtt0)*time.Millisecond/2
						if id == 1 {
							rate, delay = int64(tc.rate1*1_000_000/8/parallel), time.Duration(tc.rtt1)*time.Millisecond/2
						}
						connectTestLeg(t, left, right, id, newPacedLink(a, rate, delay), newPacedLink(b, rate, delay))
					}
					left.scheduleProbes(time.Now())
					right.scheduleProbes(time.Now())
				}
				defer func() {
					for i := range senders {
						senders[i].Close()
						receivers[i].Close()
					}
					for i := range senders {
						senders[i].workerGroup.Wait()
						receivers[i].workerGroup.Wait()
					}
				}()
				time.Sleep(250 * time.Millisecond)
				started := time.Now()
				const totalBytes = 256 << 20
				perConnection := totalBytes / parallel
				results := make(chan error, parallel*2)
				for i := range sources {
					go func() {
						payload := make([]byte, 65536)
						for sent := 0; sent < perConnection; sent += len(payload) {
							if _, err := sources[i].Write(payload); err != nil {
								results <- err
								return
							}
						}
						results <- sources[i].(closeWriter).CloseWrite()
					}()
					go func() {
						_ = targets[i].SetReadDeadline(time.Now().Add(90 * time.Second))
						n, err := io.Copy(io.Discard, targets[i])
						if err == nil && n != int64(perConnection) {
							err = fmt.Errorf("short stream: %d", n)
						}
						results <- err
					}()
				}
				for range parallel * 2 {
					if err := <-results; err != nil {
						t.Fatal(err)
					}
				}
				elapsed := time.Since(started)
				var fallback, leg1, events, backNS uint64
				for _, c := range senders {
					fallback += c.fallbackB.Load()
					leg1 += c.legCounters[1].txBytes.Load()
					events += c.replayTO.Load()
					backNS += c.backpressNS.Load()
				}
				s, r := senderMemory.snapshot(), receiverMemory.snapshot()
				mbps := float64(totalBytes) * 8 / elapsed.Seconds() / 1e6
				if mbps < tc.baselineMbps*0.98 {
					t.Errorf("throughput regression: %.1f Mbps, baseline %.1f", mbps, tc.baselineMbps)
				}
				// Healthy legs never time out. Opportunistic reinjection may
				// resend a little head data while the window or send history
				// is full, but no more than 1% of the transfer.
				if events != 0 || fallback > totalBytes/100 {
					t.Errorf("healthy reliable legs triggered replay: bytes=%d events=%d", fallback, events)
				}
				if s.PressureEvents != 0 || r.PressureEvents != 0 {
					t.Error("unused window credit caused memory pressure")
				}
				if s.PeakUsedBytes > budget || r.PeakUsedBytes > budget {
					t.Error("memory budget exceeded")
				}
				t.Logf("flows=%d duration=%v Mbps=%.1f fallback=%.2fMiB leg1=%.2fMiB timeouts=%d queue_block=%v mem_peak_TX=%.1fMiB RX=%.1fMiB pressure=%d/%d", parallel, elapsed, float64(totalBytes)*8/elapsed.Seconds()/1e6, float64(fallback)/(1<<20), float64(leg1)/(1<<20), events, time.Duration(backNS), float64(s.PeakUsedBytes)/(1<<20), float64(r.PeakUsedBytes)/(1<<20), s.PressureEvents, r.PressureEvents)
			})
		})
	}
}

func TestPerformanceColdResponse(t *testing.T) {
	for _, size := range []int{1, 4096, 65536, 1 << 20, 4 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := testCoreConfig()
				cfg.FrameSize = 65536
				cfg.QueueFrames = 256
				cfg.QueueBytes = 16 << 20
				// The old 2048-frame ceiling was above this fixture's byte cap.
				cfg.ReceiveWindowBytes = 64 << 20
				cfg.SendBufferBytes = 64 << 20
				left, app := newCore(context.Background(), cfg)
				right, peer := newCore(context.Background(), cfg)
				defer left.Close()
				defer right.Close()
				a, b := net.Pipe()
				connectTestLeg(t, left, right, 0, newPacedLink(a, 20_000_000, 32500*time.Microsecond), newPacedLink(b, 20_000_000, 32500*time.Microsecond))
				result := make(chan error, 1)
				go func() {
					request := make([]byte, 1)
					_, err := io.ReadFull(peer, request)
					if err == nil {
						_, err = peer.Write(make([]byte, size))
					}
					result <- err
				}()
				started := time.Now()
				_ = app.SetDeadline(time.Now().Add(10 * time.Second))
				if _, err := app.Write([]byte{1}); err != nil {
					t.Fatal(err)
				}
				if _, err := io.ReadFull(app, make([]byte, 1)); err != nil {
					t.Fatal(err)
				}
				first := time.Since(started)
				if _, err := io.ReadFull(app, make([]byte, size-1)); err != nil {
					t.Fatal(err)
				}
				if err := <-result; err != nil {
					t.Fatal(err)
				}
				// One leg0 RTT plus serialization, with 1 ms allowance for control bytes.
				firstBound := 65*time.Millisecond + time.Duration(min(size, 65536))*time.Second/20_000_000 + time.Millisecond
				completeBound := 65*time.Millisecond + time.Duration(size)*time.Second/20_000_000 + time.Millisecond
				if first > firstBound || time.Since(started) > completeBound {
					t.Errorf("extra startup delay: first=%v complete=%v bounds=%v/%v", first, time.Since(started), firstBound, completeBound)
				}
				t.Logf("reply=%d first=%v complete=%v", size, first, time.Since(started))
			})
		})
	}
}
