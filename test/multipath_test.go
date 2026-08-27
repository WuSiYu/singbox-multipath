package main

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-shadowsocks/shadowaead"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/json/badoption"
)

const (
	multipathTestLegDirect = "direct"
	multipathTestLegProxy  = "shadowsocks"
)

type multipathTestLeg struct {
	tag       string
	kind      string
	tfo       bool
	proxyPort uint16
}

func TestMultipathTFOCombinations(t *testing.T) {
	method := shadowaead.List[0]
	password := mkBase64(t, 16)
	legKinds := []string{multipathTestLegDirect, multipathTestLegProxy}

	for _, leg0Kind := range legKinds {
		for _, leg1Kind := range legKinds {
			for _, multipathTFO := range []bool{false, true} {
				for _, leg0TFO := range []bool{false, true} {
					for _, leg1TFO := range []bool{false, true} {
						leg0 := multipathTestLeg{
							tag:       "leg0",
							kind:      leg0Kind,
							tfo:       leg0TFO,
							proxyPort: otherPort,
						}
						leg1 := multipathTestLeg{
							tag:       "leg1",
							kind:      leg1Kind,
							tfo:       leg1TFO,
							proxyPort: otherClientPort,
						}
						name := fmt.Sprintf(
							"leg0_%s_tfo_%t/leg1_%s_tfo_%t/multipath_tfo_%t",
							leg0.kind,
							leg0.tfo,
							leg1.kind,
							leg1.tfo,
							multipathTFO,
						)
						t.Run(name, func(t *testing.T) {
							startInstance(t, multipathTFOTestOptions(multipathTFO, leg0, leg1, method, password))
							testTCP(t, clientPort, testPort)
						})
					}
				}
			}
		}
	}
}

func multipathTFOTestOptions(
	multipathTFO bool,
	leg0 multipathTestLeg,
	leg1 multipathTestLeg,
	proxyMethod string,
	proxyPassword string,
) option.Options {
	listenAddress := common.Ptr(badoption.Addr(netip.IPv4Unspecified()))
	aggregationTFO := leg0.kind == multipathTestLegDirect && leg0.tfo ||
		leg1.kind == multipathTestLegDirect && leg1.tfo
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

	inbounds := []option.Inbound{
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
					TCPFastOpen: aggregationTFO,
				},
				ActivationAfterBytes: sharedMultipathOptions.activationAfterBytes,
				ActivationWindow:     sharedMultipathOptions.activationWindow,
				ChunkSize:            sharedMultipathOptions.chunkSize,
				QueueFrames:          sharedMultipathOptions.queueFrames,
				BandwidthMbps:        sharedMultipathOptions.bandwidthMbps,
				HandshakeTimeout:     sharedMultipathOptions.handshakeTimeout,
			},
		},
	}
	outbounds := []option.Outbound{
		{
			Type:    C.TypeDirect,
			Tag:     "direct-out",
			Options: &option.DirectOutboundOptions{},
		},
	}
	for _, leg := range []multipathTestLeg{leg0, leg1} {
		dialerOptions := option.DialerOptions{
			AbstractDialerOptions: option.AbstractDialerOptions{
				TCPFastOpen: leg.tfo,
			},
		}
		if leg.kind == multipathTestLegDirect {
			outbounds = append(outbounds, option.Outbound{
				Type: C.TypeDirect,
				Tag:  leg.tag,
				Options: &option.DirectOutboundOptions{
					DialerOptions: dialerOptions,
				},
			})
			continue
		}
		inbounds = append(inbounds, option.Inbound{
			Type: C.TypeShadowsocks,
			Tag:  leg.tag + "-proxy-in",
			Options: &option.ShadowsocksInboundOptions{
				ListenOptions: option.ListenOptions{
					Listen:      listenAddress,
					ListenPort:  leg.proxyPort,
					TCPFastOpen: leg.tfo,
				},
				Method:   proxyMethod,
				Password: proxyPassword,
			},
		})
		outbounds = append(outbounds, option.Outbound{
			Type: C.TypeShadowsocks,
			Tag:  leg.tag,
			Options: &option.ShadowsocksOutboundOptions{
				ServerOptions: option.ServerOptions{
					Server:     "127.0.0.1",
					ServerPort: leg.proxyPort,
				},
				DialerOptions: dialerOptions,
				Method:        proxyMethod,
				Password:      proxyPassword,
			},
		})
	}
	outbounds = append(outbounds, option.Outbound{
		Type: C.TypeMultipath,
		Tag:  "mp-out",
		Options: &option.MultipathOutboundOptions{
			Outbounds:            []string{leg0.tag, leg1.tag},
			Preferred:            leg0.tag,
			UDPOutbound:          leg0.tag,
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
	})

	return option.Options{
		Inbounds:  inbounds,
		Outbounds: outbounds,
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
