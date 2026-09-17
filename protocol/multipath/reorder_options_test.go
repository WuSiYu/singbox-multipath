package multipath

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
)

func testPolicyOutbound(t *testing.T, data string, logger log.ContextLogger) (*Outbound, error) {
	t.Helper()
	var o option.MultipathOutboundOptions
	decoder := json.NewDecoder(strings.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&o); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	o = option.MultipathOutboundOptions{}
	if err = json.Unmarshal(encoded, &o); err != nil {
		return nil, err
	}
	o.Outbounds, o.Server, o.ServerPort = []string{"leg0", "leg1"}, "127.0.0.1", 39000
	v, err := NewOutbound(context.Background(), nil, logger, "out", o)
	if err != nil {
		return nil, err
	}
	return v.(*Outbound), nil
}

func TestBeta8DirectionalOwnership(t *testing.T) {
	o, err := testPolicyOutbound(t, `{"memory_limit":"512MB","frame_size":"16KB",
 "upload":{"aggregation_enabled":false,"queue_frames":32,"send_buffer_bytes":"4MB","receive_window_bytes":"8MB"},
 "download":{"leg0_traffic_saving":true,"activation_threshold_mbps":0,"activation_after_bytes":"2MB","queue_frames":64,"send_buffer_bytes":"16MB","receive_window_bytes":"32MB","path_stall_timeout_min":"300ms"}}`, log.NewNOPFactory().Logger())
	if err != nil {
		t.Fatal(err)
	}
	remote, err := configForPolicy(newMemoryBudget(64<<20, false), o.cfg.FrameSize, o.policy.Download, o.policy.Upload)
	if err != nil {
		t.Fatal(err)
	}
	if o.cfg.FrameSize != 16<<10 || o.cfg.AggregationEnabled || o.cfg.QueueFrames != 32 || o.cfg.SendBufferBytes != 4<<20 || o.cfg.ReceiveWindowBytes != 32<<20 {
		t.Fatalf("client: %+v", o.cfg)
	}
	if !remote.AggregationEnabled || !remote.Leg0TrafficSaving || remote.QueueFrames != 64 || remote.SendBufferBytes != 16<<20 || remote.ReceiveWindowBytes != 8<<20 || remote.PathStallTimeoutMin != 300*time.Millisecond {
		t.Fatalf("server: %+v", remote)
	}
	if remote.Memory.limit != 64<<20 {
		t.Fatal("client changed server budget")
	}
	if o.policy.Upload.AggregationEnabled == o.policy.Download.AggregationEnabled {
		t.Fatal("directions inherited each other")
	}
}

func TestBeta8AutomaticLimitsOnOwningHost(t *testing.T) {
	o, err := testPolicyOutbound(t, `{"memory_limit":"512MB"}`, log.NewNOPFactory().Logger())
	if err != nil {
		t.Fatal(err)
	}
	remote, err := configForPolicy(newMemoryBudget(64<<20, false), o.cfg.FrameSize, o.policy.Download, o.policy.Upload)
	if err != nil {
		t.Fatal(err)
	}
	if o.cfg.SendBufferBytes != 224<<20 || o.cfg.ReceiveWindowBytes != 224<<20 || remote.SendBufferBytes != 28<<20 || remote.ReceiveWindowBytes != 28<<20 {
		t.Fatal("automatic limits did not follow each host's budget")
	}
	for _, size := range []int{1024, 16384, 65536} {
		cfg, err := configForPolicy(o.cfg.Memory, size, o.policy.Upload, o.policy.Download)
		if err != nil || cfg.ReceiveWindowBytes != 224<<20 {
			t.Fatalf("frame size changed receive capacity: %d %v", size, err)
		}
	}
}

func TestBeta8LegacyFieldsIgnoredWithWarnings(t *testing.T) {
	names := []string{"aggregation_enabled", "activation_on_queue", "activation_threshold_mbps", "activation_after_bytes", "activation_after_bytes_min_mbps", "activation_window", "chunk_size", "queue_frames", "max_reorder_frames", "max_reorder_bytes", "leg1_replay_bytes", "leg1_replay_timeout", "bandwidth_mbps"}
	for _, side := range []string{"client", "server"} {
		for _, name := range names {
			for _, value := range []string{"null", "false", "-1", `"invalid"`, `{"old":true}`} {
				t.Run(side+"/"+name+"/"+value, func(t *testing.T) {
					data := `{"memory_limit":"512MB","` + name + `":` + value + `}`
					var output bytes.Buffer
					factory, err := log.New(log.Options{Context: context.Background(), Options: option.LogOptions{Level: "warn", DisableColor: true}, DefaultWriter: &output})
					if err != nil {
						t.Fatal(err)
					}
					defer factory.Close()
					logger := factory.NewLogger("test")
					if side == "client" {
						got, err := testPolicyOutbound(t, data, logger)
						if err != nil {
							t.Fatal(err)
						}
						want, err := testPolicyOutbound(t, `{"memory_limit":"512MB"}`, log.NewNOPFactory().Logger())
						if err != nil {
							t.Fatal(err)
						}
						got.cfg.Memory, want.cfg.Memory = nil, nil
						if !reflect.DeepEqual(got.cfg, want.cfg) || got.policy != want.policy {
							t.Fatal("legacy field affected policy")
						}
					} else {
						var options option.MultipathInboundOptions
						decoder := json.NewDecoder(strings.NewReader(data))
						decoder.DisallowUnknownFields()
						if err := decoder.Decode(&options); err != nil {
							t.Fatal(err)
						}
						in, err := NewInbound(context.Background(), nil, logger, "in", options)
						if err != nil {
							t.Fatal(err)
						}
						if in.(*Inbound).cfg.Memory.limit != 512<<20 {
							t.Fatal("budget changed")
						}
					}
					if err := factory.Start(); err != nil {
						t.Fatal(err)
					}
					if strings.Count(output.String(), "obsolete field "+name+" is ignored") != 1 || !strings.Contains(output.String(), "WARN") {
						t.Fatalf("missing warning: %s", output.String())
					}
				})
			}
		}
	}
}

func TestBeta8StrictNewFields(t *testing.T) {
	for _, data := range []string{
		`{"receive_window_frames":1}`, `{"download":{"receive_window_frames":1}}`,
		`{"unknown":true}`, `{"download":{"send_buffer_bytes":"bad"}}`,
		`{"frame_size":512}`, `{"download":{"queue_frames":1}}`,
		`{"upload":{"receive_window_bytes":"1KB"}}`, `{"upload":{"path_stall_timeout_min":"1ms"}}`,
		`{"download":{"activation_window":"-1s"}}`,
	} {
		if _, err := testPolicyOutbound(t, data, log.NewNOPFactory().Logger()); err == nil {
			t.Fatalf("invalid config accepted: %s", data)
		}
	}
}

func TestBeta8NewFieldsWinOverIgnoredFields(t *testing.T) {
	o, err := testPolicyOutbound(t, `{"chunk_size":"bad","aggregation_enabled":false,"download":{"leg1_replay_bytes":"bad","send_buffer_bytes":"8MB","aggregation_enabled":true}}`, log.NewNOPFactory().Logger())
	if err != nil {
		t.Fatal(err)
	}
	if o.cfg.FrameSize != 65536 || !o.policy.Download.AggregationEnabled || o.policy.Download.SendBufferBytes != 8<<20 {
		t.Fatal("new policy was changed by a tombstone")
	}
}
