package option

import (
	"strings"
	"testing"

	"github.com/sagernet/sing/common/byteformats"
	"github.com/sagernet/sing/common/json"
)

func TestMultipathFailoverClientOnly(t *testing.T) {
	for _, value := range []string{"true", "false"} {
		data := []byte(`{"failover_enabled":` + value + `}`)
		var outbound MultipathOutboundOptions
		if err := json.UnmarshalDisallowUnknownFields(data, &outbound); err != nil {
			t.Fatal(err)
		}
		if outbound.FailoverEnabled != (value == "true") {
			t.Fatalf("client flag not preserved: %s", value)
		}
		var inbound MultipathInboundOptions
		if err := json.UnmarshalDisallowUnknownFields(data, &inbound); err == nil || !strings.Contains(err.Error(), "failover_enabled") {
			t.Fatalf("server must reject the removed field (%s): %v", value, err)
		}
	}
	var inbound MultipathInboundOptions
	if err := json.UnmarshalDisallowUnknownFields([]byte(`{}`), &inbound); err != nil {
		t.Fatal(err)
	}
}

func TestMultipathByteOptions(t *testing.T) {
	var outbound MultipathOutboundOptions
	if err := json.Unmarshal([]byte(`{
		"outbounds": ["leg0", "leg1"],
		"server": "127.0.0.1",
		"server_port": 39000,
		"upload": {"activation_after_bytes": "2MB",
		"activation_after_bytes_min_mbps": 120},
		"memory_limit": "64MB"
	}`), &outbound); err != nil {
		t.Fatal(err)
	}
	if outbound.Upload.ActivationAfterBytes.Value() != 2*byteformats.MiByte {
		t.Fatalf("unexpected activation_after_bytes: %d", outbound.Upload.ActivationAfterBytes.Value())
	}
	if outbound.Upload.ActivationAfterBytesMinMbps != 120 {
		t.Fatalf("unexpected activation_after_bytes_min_mbps: %d", outbound.Upload.ActivationAfterBytesMinMbps)
	}
	if outbound.MemoryLimit.Value() != 64*byteformats.MiByte {
		t.Fatalf("unexpected memory_limit: %d", outbound.MemoryLimit.Value())
	}

	var inbound MultipathInboundOptions
	if err := json.Unmarshal([]byte(`{
		"activation_after_bytes": 4096,
		"memory_limit": 8388608
	}`), &inbound); err != nil {
		t.Fatal(err)
	}
	if len(inbound.DeprecatedActivationAfterBytes) == 0 {
		t.Fatalf("missing ignored activation_after_bytes: %s", inbound.DeprecatedActivationAfterBytes)
	}
	if inbound.MemoryLimit.Value() != 8388608 {
		t.Fatalf("unexpected numeric memory_limit: %d", inbound.MemoryLimit.Value())
	}
}
