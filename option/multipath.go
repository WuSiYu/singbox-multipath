package option

import "github.com/sagernet/sing/common/json/badoption"

// MultipathOutboundOptions combines multiple existing reliable outbounds into
// one logical TCP byte stream. UDP is intentionally not aggregated and is
// delegated to UDPOutbound (or Preferred / the first child).
type MultipathOutboundOptions struct {
	Outbounds               []string           `json:"outbounds" reference:"outbound"`
	Preferred               string             `json:"preferred,omitempty" reference:"outbound"`
	UDPOutbound             string             `json:"udp_outbound,omitempty" reference:"outbound"`
	Server                  string             `json:"server"`
	ServerPort              uint16             `json:"server_port"`
	ActivationThresholdMbps uint32             `json:"activation_threshold_mbps,omitempty"`
	ActivationWindow        badoption.Duration `json:"activation_window,omitempty"`
	ChunkSize               uint32             `json:"chunk_size,omitempty"`
	QueueFrames             uint32             `json:"queue_frames,omitempty"`
	BandwidthMbps           []uint32           `json:"bandwidth_mbps,omitempty"`
}

type MultipathInboundOptions struct {
	ListenOptions
	ActivationThresholdMbps uint32             `json:"activation_threshold_mbps,omitempty"`
	ActivationWindow        badoption.Duration `json:"activation_window,omitempty"`
	ChunkSize               uint32             `json:"chunk_size,omitempty"`
	QueueFrames             uint32             `json:"queue_frames,omitempty"`
	BandwidthMbps           []uint32           `json:"bandwidth_mbps,omitempty"`
	MaxReorderFrames        uint32             `json:"max_reorder_frames,omitempty"`
}
