package multipath

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoveryFirstHealthyPreference(t *testing.T) {
	for preferred := byte(0); preferred < 2; preferred++ {
		t.Run(fmt.Sprintf("preferred=%d", preferred), func(t *testing.T) {
			now := time.Now()
			var health [2]recoveryHealth
			other := 1 - preferred
			health[other].lastTCP, health[other].lastUDP = now, now
			health[other].refresh(now, 5*time.Second)
			choice := recoveryChoice(preferred, preferred, health, now, 30*time.Second)
			if choice != other {
				t.Fatal("startup must use the available fallback")
			}
			// TCP alone is not enough; both challenge replies remain required.
			health[preferred].lastTCP = now
			health[preferred].refresh(now, 5*time.Second)
			if recoveryChoice(choice, preferred, health, now, 30*time.Second) != other {
				t.Fatal("selected an unconfirmed path")
			}
			health[preferred].lastUDP = now.Add(100 * time.Millisecond)
			health[preferred].refresh(now.Add(100*time.Millisecond), 5*time.Second)
			choice = recoveryChoice(choice, preferred, health, now.Add(100*time.Millisecond), 30*time.Second)
			if choice != preferred {
				t.Fatal("first healthy preference incorrectly waits for failback_delay")
			}
			// A real outage after initial health must still enable the full hold.
			failed := now.Add(6 * time.Second)
			health[preferred].refresh(failed, 5*time.Second)
			health[other].lastTCP, health[other].lastUDP = failed, failed
			health[other].refresh(failed, 5*time.Second)
			choice = recoveryChoice(choice, preferred, health, failed, 30*time.Second)
			if choice != other {
				t.Fatal("failed to leave the unavailable preferred path")
			}
			recovered := failed.Add(time.Second)
			health[preferred].lastTCP, health[preferred].lastUDP = recovered, recovered
			health[preferred].refresh(recovered, 5*time.Second)
			for second := 0; second <= 30; second++ {
				at := recovered.Add(time.Duration(second) * time.Second)
				for id := range health {
					health[id].lastTCP, health[id].lastUDP = at, at
					health[id].refresh(at, 5*time.Second)
				}
				want := other
				if second == 30 {
					want = preferred
				}
				if got := recoveryChoice(choice, preferred, health, at, 30*time.Second); got != want {
					t.Fatalf("real recovery at %ds: got=%d want=%d", second, got, want)
				}
			}
		})
	}
}

func TestStatusRequiresSameSessionBooster(t *testing.T) {
	for _, mode := range []uint64{2, 3} {
		t.Run(dataModeName(mode), func(t *testing.T) {
			cfg := testCoreConfig()
			cfg.Memory = newMemoryBudget(64<<20, false)
			left, _ := newCore(context.Background(), cfg)
			right, _ := newCore(context.Background(), cfg)
			defer left.Close()
			defer right.Close()
			a, b := net.Pipe()
			connectTestLeg(t, left, right, 0, a, b)
			status := newOutboundStatus(filepath.Join(t.TempDir(), "status.json"), outboundStatusConfig{cfg: cfg})
			id := [16]byte{1}
			status.addSession(id, "example.com:443", left, leg1PhaseReady)
			now := time.Now()
			left.handlePeerSenderStatus(senderStatus{Sequence: 1, DataMode: mode, Flags: senderStatusFlagActive | senderStatusFlagLeg0Present | senderStatusFlagLeg1Present}, now)
			checkMissing := func() {
				t.Helper()
				doc := status.buildDocument(now)
				if doc.Node.Logical.State != "booster_degraded" || doc.Node.Logical.DownloadStates["unknown"] != 1 || doc.Node.Logical.RXAggregatingConnections != 0 {
					t.Fatalf("absent booster treated as active: %+v", doc.Node.Logical)
				}
			}
			checkMissing()
			// Another flow's booster cannot validate this flow's remote mode.
			other, _ := newCore(context.Background(), cfg)
			otherPeer, _ := newCore(context.Background(), cfg)
			defer other.Close()
			defer otherPeer.Close()
			for id := uint8(0); id < 2; id++ {
				x, y := net.Pipe()
				connectTestLeg(t, other, otherPeer, id, x, y)
			}
			other.handlePeerSenderStatus(senderStatus{Sequence: 1, DataMode: 1}, now)
			status.addSession([16]byte{2}, "other.example:443", other, leg1PhaseReady)
			checkMissing()
			// An attached idle booster is valid even with zero DATA throughput.
			x, y := net.Pipe()
			connectTestLeg(t, left, right, 1, x, y)
			doc := status.buildDocument(now)
			want := "aggregating"
			if mode == 3 {
				want = "traffic_saving"
			}
			if doc.Node.Logical.State != want || doc.Node.Logical.DownloadStates[dataModeName(mode)] != 1 {
				t.Fatalf("attached idle booster was hidden: %+v", doc.Node.Logical)
			}
			doc = status.buildDocument(now.Add(4 * time.Second))
			if doc.Node.Logical.State == want || doc.Node.Logical.DownloadStates[dataModeName(mode)] != 0 {
				t.Fatal("stale telemetry treated as active")
			}
		})
	}
}
