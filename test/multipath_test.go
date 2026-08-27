package main

import (
	"net/netip"
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/json/badoption"
)

func TestMultipathTFOCombinations(t *testing.T) {
	testCases := []struct {
		name              string
		multipathTFO      bool
		childAndServerTFO bool
	}{
		{name: "multipath_off_child_off"},
		{name: "multipath_off_child_on", childAndServerTFO: true},
		{name: "multipath_on_child_off", multipathTFO: true},
		{name: "multipath_on_child_on", multipathTFO: true, childAndServerTFO: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			startInstance(t, multipathTFOTestOptions(testCase.multipathTFO, testCase.childAndServerTFO))
			testTCP(t, clientPort, testPort)
		})
	}
}

func multipathTFOTestOptions(multipathTFO bool, childAndServerTFO bool) option.Options {
	listenAddress := common.Ptr(badoption.Addr(netip.IPv4Unspecified()))
	childDialerOptions := option.DialerOptions{
		AbstractDialerOptions: option.AbstractDialerOptions{
			TCPFastOpen: childAndServerTFO,
		},
	}
	sharedMultipathOptions := struct {
		activationAfterBytes uint64
		activationWindow     badoption.Duration
		chunkSize            uint32
		queueFrames          uint32
		bandwidthMbps        []uint32
		handshakeTimeout     badoption.Duration
	}{
		activationAfterBytes: 1,
		activationWindow:     badoption.Duration(time.Second),
		chunkSize:            16 * 1024,
		queueFrames:          64,
		bandwidthMbps:        []uint32{100, 100},
		handshakeTimeout:     badoption.Duration(5 * time.Second),
	}

	return option.Options{
		Inbounds: []option.Inbound{
			{
				Type: C.TypeMixed,
				Tag:  "mixed-in",
				Options: &option.HTTPMixedInboundOptions{
					ListenOptions: option.ListenOptions{
						Listen:     listenAddress,
						ListenPort: clientPort,
					},
				},
			},
			{
				Type: C.TypeMultipath,
				Tag:  "multipath-in",
				Options: &option.MultipathInboundOptions{
					ListenOptions: option.ListenOptions{
						Listen:      listenAddress,
						ListenPort:  serverPort,
						TCPFastOpen: childAndServerTFO,
					},
					ActivationAfterBytes: sharedMultipathOptions.activationAfterBytes,
					ActivationWindow:     sharedMultipathOptions.activationWindow,
					ChunkSize:            sharedMultipathOptions.chunkSize,
					QueueFrames:          sharedMultipathOptions.queueFrames,
					BandwidthMbps:        sharedMultipathOptions.bandwidthMbps,
					HandshakeTimeout:     sharedMultipathOptions.handshakeTimeout,
				},
			},
		},
		Outbounds: []option.Outbound{
			{
				Type:    C.TypeDirect,
				Tag:     "direct-out",
				Options: &option.DirectOutboundOptions{},
			},
			{
				Type: C.TypeDirect,
				Tag:  "leg0",
				Options: &option.DirectOutboundOptions{
					DialerOptions: childDialerOptions,
				},
			},
			{
				Type: C.TypeDirect,
				Tag:  "leg1",
				Options: &option.DirectOutboundOptions{
					DialerOptions: childDialerOptions,
				},
			},
			{
				Type: C.TypeMultipath,
				Tag:  "mp-out",
				Options: &option.MultipathOutboundOptions{
					Outbounds:            []string{"leg0", "leg1"},
					Preferred:            "leg0",
					UDPOutbound:          "leg0",
					Server:               "127.0.0.1",
					ServerPort:           serverPort,
					TCPFastOpen:          multipathTFO,
					ActivationAfterBytes: sharedMultipathOptions.activationAfterBytes,
					ActivationWindow:     sharedMultipathOptions.activationWindow,
					ChunkSize:            sharedMultipathOptions.chunkSize,
					QueueFrames:          sharedMultipathOptions.queueFrames,
					BandwidthMbps:        sharedMultipathOptions.bandwidthMbps,
					HandshakeTimeout:     sharedMultipathOptions.handshakeTimeout,
				},
			},
		},
		Route: &option.RouteOptions{
			Rules: []option.Rule{
				{
					Type: C.RuleTypeDefault,
					DefaultOptions: option.DefaultRule{
						RawDefaultRule: option.RawDefaultRule{
							Inbound: []string{"mixed-in"},
						},
						RuleAction: option.RuleAction{
							Action: C.RuleActionTypeRoute,
							RouteOptions: option.RouteActionOptions{
								Outbound: "mp-out",
							},
						},
					},
				},
			},
			Final: "direct-out",
		},
	}
}
