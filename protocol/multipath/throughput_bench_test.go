package multipath

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
)

// Real loopback TCP: exercises framing, allocation and ACK processing without
// simulating propagation delay. This measures local CPU cost, not WAN speed.
func BenchmarkCoreTCPThroughput(b *testing.B) {
	for _, legs := range []int{1, 2} {
		b.Run(fmt.Sprint(legs), func(b *testing.B) {
			cfg := testCoreConfig()
			cfg.FrameSize, cfg.QueueFrames, cfg.QueueBytes = 65536, 256, 16<<20
			cfg.ReceiveWindowBytes, cfg.SendBufferBytes = 64<<20, 64<<20
			left, app := newCore(context.Background(), cfg)
			right, peer := newCore(context.Background(), cfg)
			defer left.Close()
			defer right.Close()
			if legs == 2 {
				left.activate(activationInfo{Reason: activationReasonBytes})
			}
			for id := 0; id < legs; id++ {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					b.Fatal(err)
				}
				client, err := net.Dial("tcp", listener.Addr().String())
				if err != nil {
					listener.Close()
					b.Fatal(err)
				}
				server, err := listener.Accept()
				listener.Close()
				if err != nil {
					client.Close()
					b.Fatal(err)
				}
				if _, err = left.addLeg(uint8(id), client, nil); err != nil {
					b.Fatal(err)
				}
				if _, err = right.addLeg(uint8(id), server, nil); err != nil {
					b.Fatal(err)
				}
			}
			payload := make([]byte, 65536)
			const transfer = 64 << 20
			b.SetBytes(transfer)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				done := make(chan error, 1)
				go func() { _, err := io.CopyN(io.Discard, peer, transfer); done <- err }()
				for sent := 0; sent < transfer; sent += len(payload) {
					if _, err := app.Write(payload); err != nil {
						b.Fatal(err)
					}
				}
				if err := <-done; err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
