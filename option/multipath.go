package option

import (
	"github.com/sagernet/sing/common/byteformats"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badoption"
)

// Direction names use the client's perspective. ReceiveWindowBytes belongs to
// that direction's receiver; automatic limits are resolved on the owning host.
type MultipathDirectionOptions struct {
	AggregationEnabled          *bool                    `json:"aggregation_enabled,omitempty"`
	Leg0TrafficSaving           bool                     `json:"leg0_traffic_saving,omitempty"`
	ActivationOnQueue           *bool                    `json:"activation_on_queue,omitempty"`
	ActivationThresholdMbps     *uint32                  `json:"activation_threshold_mbps,omitempty"`
	ActivationAfterBytes        *byteformats.MemoryBytes `json:"activation_after_bytes,omitempty"`
	ActivationAfterBytesMinMbps uint32                   `json:"activation_after_bytes_min_mbps,omitempty"`
	ActivationWindow            badoption.Duration       `json:"activation_window,omitempty"`
	QueueFrames                 uint32                   `json:"queue_frames,omitempty"`
	SendBufferBytes             *byteformats.MemoryBytes `json:"send_buffer_bytes,omitempty"`
	ReceiveWindowBytes          *byteformats.MemoryBytes `json:"receive_window_bytes,omitempty"`
	PathStallTimeoutMin         badoption.Duration       `json:"path_stall_timeout_min,omitempty"`
	MultipathDeprecatedNames
}

// Parsing tombstones, never aliases. Any valid old JSON value is warned about
// and ignored, without type validation or participation in the effective policy.
type MultipathDeprecatedNames struct {
	DeprecatedChunkSize         json.RawMessage `json:"chunk_size,omitempty"`
	DeprecatedMaxReorderFrames  json.RawMessage `json:"max_reorder_frames,omitempty"`
	DeprecatedMaxReorderBytes   json.RawMessage `json:"max_reorder_bytes,omitempty"`
	DeprecatedLeg1ReplayBytes   json.RawMessage `json:"leg1_replay_bytes,omitempty"`
	DeprecatedLeg1ReplayTimeout json.RawMessage `json:"leg1_replay_timeout,omitempty"`
	DeprecatedBandwidthMbps     json.RawMessage `json:"bandwidth_mbps,omitempty"`
}

type MultipathDeprecatedFlatOptions struct {
	MultipathDeprecatedNames
	DeprecatedAggregationEnabled          json.RawMessage `json:"aggregation_enabled,omitempty"`
	DeprecatedActivationOnQueue           json.RawMessage `json:"activation_on_queue,omitempty"`
	DeprecatedActivationThresholdMbps     json.RawMessage `json:"activation_threshold_mbps,omitempty"`
	DeprecatedActivationAfterBytes        json.RawMessage `json:"activation_after_bytes,omitempty"`
	DeprecatedActivationAfterBytesMinMbps json.RawMessage `json:"activation_after_bytes_min_mbps,omitempty"`
	DeprecatedActivationWindow            json.RawMessage `json:"activation_window,omitempty"`
	DeprecatedQueueFrames                 json.RawMessage `json:"queue_frames,omitempty"`
}

type MultipathOutboundOptions struct {
	Outbounds        []string                  `json:"outbounds" reference:"outbound"`
	Preferred        string                    `json:"preferred,omitempty" reference:"outbound"`
	UDPOutbound      string                    `json:"udp_outbound,omitempty" reference:"outbound"`
	Server           string                    `json:"server"`
	ServerPort       uint16                    `json:"server_port"`
	TCPFastOpen      *bool                     `json:"tcp_fast_open,omitempty"`
	PSK              string                    `json:"psk,omitempty"`
	FailoverEnabled  bool                      `json:"failover_enabled,omitempty"`
	FailoverTimeout  badoption.Duration        `json:"failover_timeout,omitempty"`
	FailbackDelay    badoption.Duration        `json:"failback_delay,omitempty"`
	FrameSize        *byteformats.MemoryBytes  `json:"frame_size,omitempty"`
	Upload           MultipathDirectionOptions `json:"upload,omitempty"`
	Download         MultipathDirectionOptions `json:"download,omitempty"`
	MemoryLimit      *byteformats.MemoryBytes  `json:"memory_limit,omitempty"`
	HandshakeTimeout badoption.Duration        `json:"handshake_timeout,omitempty"`
	StatusFile       string                    `json:"status_file,omitempty"`
	MultipathDeprecatedFlatOptions
}

type MultipathInboundOptions struct {
	ListenOptions
	MemoryLimit      *byteformats.MemoryBytes                 `json:"memory_limit,omitempty"`
	HandshakeTimeout badoption.Duration                       `json:"handshake_timeout,omitempty"`
	PSK              string                                   `json:"psk,omitempty"`
	AllowedIPs       badoption.Listable[badoption.Prefixable] `json:"allowed_ips,omitempty"`
	MultipathDeprecatedFlatOptions
}
