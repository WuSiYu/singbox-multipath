package multipath

import (
	"context"
	"fmt"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
)

func TestActivationOptionsDefaultsAndExplicitZero(t *testing.T) {
	for _, test := range []struct {
		name, options  string
		enabled, queue bool
		rate           uint64
	}{
		{"defaults", `{}`, true, true, 150_000_000 / 8},
		{"explicit_zero", `{"activation_threshold_mbps":0}`, true, true, 0},
		{"byte_default", `{"activation_after_bytes":"2MB"}`, true, true, 0},
		{"byte_zero", `{"activation_after_bytes":0}`, true, true, 150_000_000 / 8},
		{"all_triggers_off", `{"activation_on_queue":false,"activation_threshold_mbps":0,"activation_after_bytes":0}`, true, false, 0},
		{"master_off", `{"aggregation_enabled":false}`, false, true, 150_000_000 / 8},
		{"explicit_on", `{"aggregation_enabled":true,"activation_on_queue":true,"activation_threshold_mbps":120}`, true, true, 120_000_000 / 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			var direction option.MultipathDirectionOptions
			if err := json.Unmarshal([]byte(test.options), &direction); err != nil {
				t.Fatal(err)
			}
			outboundOptions := option.MultipathOutboundOptions{Upload: direction, Download: direction}
			// Marshal/unmarshal must retain explicit false/zero as well as omission.
			encoded, err := json.Marshal(outboundOptions)
			if err != nil {
				t.Fatal(err)
			}
			outboundOptions = option.MultipathOutboundOptions{}
			if err = json.Unmarshal(encoded, &outboundOptions); err != nil {
				t.Fatal(err)
			}
			outboundOptions.Outbounds = []string{"leg0", "leg1"}
			outboundOptions.Server, outboundOptions.ServerPort = "127.0.0.1", 39000
			logger := log.NewNOPFactory().Logger()
			outbound, err := NewOutbound(context.Background(), nil, logger, "out", outboundOptions)
			if err != nil {
				t.Fatal(err)
			}
			remote, err := configForPolicy(outbound.(*Outbound).cfg.Memory, outbound.(*Outbound).cfg.FrameSize, outbound.(*Outbound).policy.Download, outbound.(*Outbound).policy.Upload)
			if err != nil {
				t.Fatal(err)
			}
			for side, cfg := range map[string]coreConfig{"server": remote, "client": outbound.(*Outbound).cfg} {
				if cfg.AggregationEnabled != test.enabled || cfg.ActivationOnQueue != test.queue || cfg.ThresholdBytesPS != test.rate {
					t.Fatalf("%s: enabled=%t queue=%t rate=%d", side, cfg.AggregationEnabled, cfg.ActivationOnQueue, cfg.ThresholdBytesPS)
				}
			}
		})
	}
}

func TestActivationTriggerORCombinations(t *testing.T) {
	// Each stimulus meets exactly one trigger. Other triggers can be enabled but
	// deliberately remain below their thresholds, including the byte-rate gate.
	for _, enabled := range []bool{false, true} {
		for mask := 0; mask < 8; mask++ {
			for _, stimulus := range []string{"none", "queue", "rate", "bytes", "bytes_below_min"} {
				name := fmt.Sprintf("enabled=%t/mask=%d/%s", enabled, mask, stimulus)
				t.Run(name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						cfg := testCoreConfig()
						cfg.AggregationEnabled = enabled
						cfg.ActivationOnQueue = mask&1 != 0
						cfg.ActivationWindow = 100 * time.Millisecond
						if mask&2 != 0 {
							cfg.ThresholdBytesPS = 100_000_000
						}
						if mask&4 != 0 {
							cfg.ActivationAfterBytes = 1_000_000
						}
						cfg.ActivationAfterBytesMinBytesPS = 1_000_000
						if stimulus == "rate" && mask&2 != 0 {
							cfg.ThresholdBytesPS = 1_000_000
						}
						if stimulus == "bytes_below_min" {
							cfg.ActivationAfterBytesMinBytesPS = 100_000_000
						}
						core, _ := newCore(context.Background(), cfg)
						defer core.Close()
						wire, peer := net.Pipe()
						defer peer.Close()
						leg, err := core.addLeg(0, wire, nil)
						if err != nil {
							t.Fatal(err)
						}
						synctest.Wait()
						if core.active.Load() {
							t.Fatal("activated before any trigger")
						}
						switch stimulus {
						case "queue":
							leg.queuedBytes.Store(cfg.QueueBytes*4/5 + 1)
						case "rate":
							core.ingressBytes.Store(200_000)
						case "bytes", "bytes_below_min":
							core.ingressBytes.Store(2_000_000)
						}
						time.Sleep(400 * time.Millisecond)
						synctest.Wait()
						want := enabled && (stimulus == "queue" && mask&1 != 0 || stimulus == "rate" && mask&2 != 0 || stimulus == "bytes" && mask&4 != 0)
						if core.active.Load() != want {
							t.Fatalf("active=%t, want %t", core.active.Load(), want)
						}
						if !enabled {
							core.activate(activationInfo{Reason: activationReasonThroughput})
							if core.active.Load() {
								t.Fatal("activation bypassed the master switch")
							}
						}
					})
				})
			}
		}
	}
}

func TestActivationStatusFlags(t *testing.T) {
	cfg := testCoreConfig()
	cfg.AggregationEnabled = false
	cfg.ActivationOnQueue = false
	cfg.Memory = newMemoryBudget(8<<20, false)
	status := newOutboundStatus("", outboundStatusConfig{cfg: cfg})
	document := status.buildDocument(time.Now())
	encoded, err := json.Marshal(document.Node.Parameters.Upload)
	if err != nil {
		t.Fatal(err)
	}
	var parameters map[string]any
	if err = json.Unmarshal(encoded, &parameters); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"aggregation_enabled", "activation_on_queue"} {
		value, ok := parameters[key]
		if !ok || value != false {
			t.Fatalf("%s must be explicitly false in telemetry: %s", key, encoded)
		}
	}
}
