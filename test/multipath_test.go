package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-shadowsocks/shadowaead"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/byteformats"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/protocol/socks"
)

const (
	multipathTestLegDirect = "direct"
	multipathTestLegProxy  = "shadowsocks"
)

func multipathMemoryBytes(value string) *byteformats.MemoryBytes {
	var result byteformats.MemoryBytes
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		panic(err)
	}
	return &result
}

type multipathTestLeg struct {
	tag       string
	kind      string
	tfo       bool
	proxyPort uint16
}

func TestMultipathTFOCombinations(t *testing.T) {
	testMultipathTFOCombinations(t, false)
}

func TestMultipathTrafficSavingTFOCombinations(t *testing.T) {
	testMultipathTFOCombinations(t, true)
}

func testMultipathTFOCombinations(t *testing.T, saving bool) {
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
							opts := multipathTFOTestOptions(multipathTFO, leg0, leg1, method, password)
							mp := opts.Outbounds[len(opts.Outbounds)-1].Options.(*option.MultipathOutboundOptions)
							mp.Upload.Leg0TrafficSaving, mp.Download.Leg0TrafficSaving = saving, saving
							startInstance(t, opts)
							testTCP(t, clientPort, testPort)
						})
					}
				}
			}
		}
	}
}

func TestMultipathDirectionalAggregation(t *testing.T) {
	for _, clientEnabled := range []bool{false, true} {
		for _, serverEnabled := range []bool{false, true} {
			for _, fastOpen := range []bool{false, true} {
				t.Run(fmt.Sprintf("client=%t/server=%t/tfo=%t", clientEnabled, serverEnabled, fastOpen), func(t *testing.T) {
					listener, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					payload := bytes.Repeat([]byte("directional-aggregation"), 65536)
					serverDone := make(chan error, 1)
					go func() {
						conn, acceptErr := listener.Accept()
						if acceptErr != nil {
							serverDone <- acceptErr
							return
						}
						defer conn.Close()
						_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
						warmup := make([]byte, 1)
						if _, acceptErr = io.ReadFull(conn, warmup); acceptErr == nil {
							_, acceptErr = conn.Write(warmup)
						}
						received := make([]byte, len(payload))
						if acceptErr == nil {
							_, acceptErr = io.ReadFull(conn, received)
						}
						if acceptErr == nil && !bytes.Equal(payload, received) {
							acceptErr = fmt.Errorf("upload payload mismatch")
						}
						if acceptErr == nil {
							_, acceptErr = conn.Write(payload)
						}
						serverDone <- acceptErr
					}()
					leg0 := multipathTestLeg{tag: "leg0", kind: multipathTestLegDirect, tfo: fastOpen}
					leg1 := multipathTestLeg{tag: "leg1", kind: multipathTestLegProxy, tfo: fastOpen, proxyPort: otherClientPort}
					options := multipathTFOTestOptions(fastOpen, leg0, leg1, shadowaead.List[0], mkBase64(t, 16))
					outbound := options.Outbounds[len(options.Outbounds)-1].Options.(*option.MultipathOutboundOptions)
					outbound.Upload.AggregationEnabled = common.Ptr(clientEnabled)
					outbound.Download.AggregationEnabled = common.Ptr(serverEnabled)
					outbound.Upload.ActivationOnQueue = common.Ptr(false)
					outbound.Upload.ActivationThresholdMbps = common.Ptr(uint32(0))
					outbound.Upload.ActivationWindow = badoption.Duration(50 * time.Millisecond)
					outbound.Download.ActivationOnQueue = outbound.Upload.ActivationOnQueue
					outbound.Download.ActivationThresholdMbps = outbound.Upload.ActivationThresholdMbps
					outbound.Download.ActivationWindow = outbound.Upload.ActivationWindow
					outbound.StatusFile = filepath.Join(t.TempDir(), "multipath.json")
					startInstance(t, options)
					dialer := socks.NewClient(N.SystemDialer, M.ParseSocksaddrHostPort("127.0.0.1", clientPort), socks.Version5, "", "")
					conn, err := dialer.DialContext(context.Background(), "tcp", M.ParseSocksaddr(listener.Addr().String()))
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
					if _, err = conn.Write([]byte{1}); err != nil {
						t.Fatal(err)
					}
					if _, err = io.ReadFull(conn, make([]byte, 1)); err != nil {
						t.Fatal(err)
					}
					var document struct {
						Node struct {
							Logical struct {
								TXAggregating int `json:"tx_aggregating_connections"`
							} `json:"logical"`
							Legs []struct {
								Connections int `json:"connections"`
								Cumulative  struct {
									TX uint64 `json:"tx_bytes"`
									RX uint64 `json:"rx_bytes"`
								} `json:"cumulative"`
							} `json:"legs"`
						} `json:"node"`
					}
					readStatus := func() bool {
						data, readErr := os.ReadFile(outbound.StatusFile)
						return readErr == nil && json.Unmarshal(data, &document) == nil && len(document.Node.Legs) == 2
					}
					deadline := time.Now().Add(5 * time.Second)
					for !readStatus() || document.Node.Legs[1].Connections != 1 || (document.Node.Logical.TXAggregating > 0) != clientEnabled {
						if time.Now().After(deadline) {
							t.Fatal("leg1 did not attach or local activation state is incorrect")
						}
						time.Sleep(20 * time.Millisecond)
					}
					if _, err = conn.Write(payload); err != nil {
						t.Fatal(err)
					}
					received := make([]byte, len(payload))
					if _, err = io.ReadFull(conn, received); err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(payload, received) {
						t.Fatal("download payload mismatch")
					}
					if err = <-serverDone; err != nil {
						t.Fatal(err)
					}
					deadline = time.Now().Add(5 * time.Second)
					for {
						if readStatus() {
							leg0, leg1 := document.Node.Legs[0], document.Node.Legs[1]
							if leg0.Cumulative.TX+leg1.Cumulative.TX >= uint64(len(payload)+1) && leg0.Cumulative.RX+leg1.Cumulative.RX >= uint64(len(payload)+1) {
								if (leg1.Cumulative.TX > 0) != clientEnabled || (leg1.Cumulative.RX > 0) != serverEnabled {
									t.Fatalf("leg1 traffic does not match directional switches: TX=%d RX=%d", leg1.Cumulative.TX, leg1.Cumulative.RX)
								}
								break
							}
						}
						if time.Now().After(deadline) {
							t.Fatal("final directional traffic was not recorded")
						}
						time.Sleep(20 * time.Millisecond)
					}
				})
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
		frameSize            uint32
		queueFrames          uint32
		handshakeTimeout     badoption.Duration
	}{
		activationAfterBytes: 1,
		activationWindow:     badoption.Duration(time.Second),
		frameSize:            16 * 1024,
		queueFrames:          64,
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
				HandshakeTimeout: sharedMultipathOptions.handshakeTimeout,
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
			Outbounds:        []string{leg0.tag, leg1.tag},
			Preferred:        leg0.tag,
			UDPOutbound:      leg0.tag,
			Server:           "127.0.0.1",
			ServerPort:       serverPort,
			TCPFastOpen:      multipathTFO,
			FrameSize:        multipathMemoryBytes(fmt.Sprint(sharedMultipathOptions.frameSize)),
			Upload:           option.MultipathDirectionOptions{ActivationAfterBytes: multipathMemoryBytes("1"), ActivationWindow: sharedMultipathOptions.activationWindow, QueueFrames: sharedMultipathOptions.queueFrames},
			Download:         option.MultipathDirectionOptions{ActivationAfterBytes: multipathMemoryBytes("1"), ActivationWindow: sharedMultipathOptions.activationWindow, QueueFrames: sharedMultipathOptions.queueFrames},
			HandshakeTimeout: sharedMultipathOptions.handshakeTimeout,
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
