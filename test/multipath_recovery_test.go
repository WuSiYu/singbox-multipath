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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/direct"
	"github.com/sagernet/sing-shadowsocks/shadowaead"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/common/pipe"
)

// Faults stall reliable bytes without returning EOF, and drop native datagrams.
// No host routes, firewall rules or live interfaces are modified by these tests.
type recoveryFaultChild struct {
	*direct.Outbound
	down *atomic.Bool
}
type recoveryFaultConn struct {
	net.Conn
	down        *atomic.Bool
	udp         bool
	done        chan struct{}
	once        sync.Once
	read, write pipe.Deadline
}

func (c *recoveryFaultConn) Upstream() any { return c.Conn }
func (c *recoveryFaultChild) DialContext(ctx context.Context, n string, d M.Socksaddr) (net.Conn, error) {
	conn, err := c.Outbound.DialContext(ctx, n, d)
	if err != nil {
		return nil, err
	}
	return &recoveryFaultConn{Conn: conn, down: c.down, udp: strings.HasPrefix(n, "udp"), done: make(chan struct{}), read: pipe.MakeDeadline(), write: pipe.MakeDeadline()}, nil
}
func (c *recoveryFaultConn) wait(d *pipe.Deadline) error {
	for c.down.Load() {
		select {
		case <-c.done:
			return net.ErrClosed
		case <-d.Wait():
			return os.ErrDeadlineExceeded
		case <-time.After(5 * time.Millisecond):
		}
	}
	select {
	case <-c.done:
		return net.ErrClosed
	default:
		return nil
	}
}
func (c *recoveryFaultConn) Write(p []byte) (int, error) {
	if c.udp && c.down.Load() {
		return len(p), nil
	}
	if err := c.wait(&c.write); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}
func (c *recoveryFaultConn) Read(p []byte) (int, error) {
	for {
		n, err := c.Conn.Read(p)
		if err != nil {
			return n, err
		}
		if c.udp && c.down.Load() {
			continue
		}
		if err = c.wait(&c.read); err != nil {
			return 0, err
		}
		return n, nil
	}
}
func (c *recoveryFaultConn) Close() error { c.once.Do(func() { close(c.done) }); return c.Conn.Close() }
func (c *recoveryFaultConn) SetDeadline(t time.Time) error {
	c.read.Set(t)
	c.write.Set(t)
	return c.Conn.SetDeadline(t)
}
func (c *recoveryFaultConn) SetReadDeadline(t time.Time) error {
	c.read.Set(t)
	return c.Conn.SetReadDeadline(t)
}
func (c *recoveryFaultConn) SetWriteDeadline(t time.Time) error {
	c.write.Set(t)
	return c.Conn.SetWriteDeadline(t)
}

type recoveryCountingChild struct {
	adapter.Outbound
	calls *atomic.Int32
}

func (c *recoveryCountingChild) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	c.calls.Add(1)
	return c.Outbound.DialContext(ctx, network, destination)
}

func TestMultipathRecoveryClientDisabledHasNoProbes(t *testing.T) {
	var calls atomic.Int32
	registry := include.OutboundRegistry()
	outbound.Register[option.DirectOutboundOptions](registry, "recovery-count", func(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, opts option.DirectOutboundOptions) (adapter.Outbound, error) {
		child, err := direct.NewOutbound(ctx, router, logger, tag, opts)
		return &recoveryCountingChild{child, &calls}, err
	})
	ctx, cancel := context.WithCancel(box.Context(context.Background(), include.InboundRegistry(), registry, include.EndpointRegistry(), include.DNSTransportRegistry(), include.ServiceRegistry(), include.CertificateProviderRegistry()))
	defer cancel()
	opts := multipathTFOTestOptions(false, multipathTestLeg{tag: "leg0", kind: multipathTestLegDirect}, multipathTestLeg{tag: "leg1", kind: multipathTestLegDirect}, shadowaead.List[0], mkBase64(t, 16))
	for j := range opts.Outbounds {
		if opts.Outbounds[j].Tag == "leg0" || opts.Outbounds[j].Tag == "leg1" {
			opts.Outbounds[j].Type = "recovery-count"
		}
	}
	opts.Log = &option.LogOptions{Level: "error"}
	instance, err := box.New(box.Options{Context: ctx, Options: opts})
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	if err = instance.Start(); err != nil {
		t.Fatal(err)
	}
	// Recovery would send its first probes immediately and repeat every second.
	time.Sleep(1200 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("disabled recovery dialed children %d times", calls.Load())
	}
	udp, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", serverPort))
	if err == nil {
		udp.Close()
		t.Fatal("server must listen on UDP regardless of the client recovery flag")
	}
}

func recoveryTestInstance(t *testing.T, fast, secondaryUDP, startDown bool, useHY2 ...bool) (*box.Box, *atomic.Bool, string) {
	t.Helper()
	down := new(atomic.Bool)
	down.Store(startDown)
	registry := include.OutboundRegistry()
	outbound.Register[option.DirectOutboundOptions](registry, "recovery-fault", func(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, opts option.DirectOutboundOptions) (adapter.Outbound, error) {
		o, err := direct.NewOutbound(ctx, router, logger, tag, opts)
		if err != nil {
			return nil, err
		}
		return &recoveryFaultChild{o.(*direct.Outbound), down}, nil
	})
	ctx, cancel := context.WithCancel(box.Context(context.Background(), include.InboundRegistry(), registry, include.EndpointRegistry(), include.DNSTransportRegistry(), include.ServiceRegistry(), include.CertificateProviderRegistry()))
	leg0 := multipathTestLeg{tag: "leg0", kind: multipathTestLegDirect, tfo: fast}
	leg1 := multipathTestLeg{tag: "leg1", kind: multipathTestLegDirect, tfo: fast}
	opts := multipathTFOTestOptions(fast, leg0, leg1, shadowaead.List[0], mkBase64(t, 16))
	if len(useHY2) > 0 && useHY2[0] {
		_, cert, key := createSelfSignedCertificate(t, "example.org")
		opts.Inbounds = append(opts.Inbounds, option.Inbound{
			Type: C.TypeHysteria2, Tag: "recovery-hy2-in",
			Options: &option.Hysteria2InboundOptions{
				ListenOptions: option.ListenOptions{Listen: common.Ptr(badoption.Addr(netip.IPv4Unspecified())), ListenPort: otherClientPort},
				UpMbps:        100, DownMbps: 100,
				Users:                      []option.Hysteria2User{{Password: "recovery-test"}},
				InboundTLSOptionsContainer: option.InboundTLSOptionsContainer{TLS: &option.InboundTLSOptions{Enabled: true, ServerName: "example.org", CertificatePath: cert, KeyPath: key}},
			},
		})
		for j := range opts.Outbounds {
			if opts.Outbounds[j].Tag == "leg1" {
				opts.Outbounds[j].Type = C.TypeHysteria2
				opts.Outbounds[j].Options = &option.Hysteria2OutboundOptions{
					ServerOptions: option.ServerOptions{Server: "127.0.0.1", ServerPort: otherClientPort},
					UpMbps:        100, DownMbps: 100, Password: "recovery-test",
					OutboundTLSOptionsContainer: option.OutboundTLSOptionsContainer{TLS: &option.OutboundTLSOptions{Enabled: true, ServerName: "example.org", CertificatePath: cert}},
				}
			}
		}
	}
	opts.Log = &option.LogOptions{Level: "error"}
	for j := range opts.Outbounds {
		if opts.Outbounds[j].Tag == "leg0" {
			opts.Outbounds[j].Type = "recovery-fault"
		}
	}
	out := opts.Outbounds[len(opts.Outbounds)-1].Options.(*option.MultipathOutboundOptions)
	out.FailoverEnabled = true
	out.FailoverTimeout = badoption.Duration(time.Second)
	out.FailbackDelay = badoption.Duration(2 * time.Second)
	disabled := false
	out.Upload.AggregationEnabled = &disabled
	out.Download.AggregationEnabled = &disabled
	if len(useHY2) > 1 && useHY2[1] {
		enabled := true
		out.Upload.AggregationEnabled, out.Download.AggregationEnabled = &enabled, &enabled
		out.Upload.Leg0TrafficSaving, out.Download.Leg0TrafficSaving = true, true
	}
	if secondaryUDP {
		out.UDPOutbound = "leg1"
	}
	out.StatusFile = filepath.Join(t.TempDir(), "recovery.json")
	instance, err := box.New(box.Options{Context: ctx, Options: opts})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err = instance.Start(); err != nil {
		instance.Close()
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Close(); cancel() })
	return instance, down, out.StatusFile
}

func recoveryWaitStatus(t *testing.T, file string, tcp, udp int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		var doc struct {
			Node struct {
				Recovery struct {
					TCP  int `json:"tcp_path"`
					UDP  int `json:"udp_path"`
					Mask int `json:"usable_mask"`
				} `json:"recovery"`
			} `json:"node"`
		}
		p, err := os.ReadFile(file)
		if err == nil && json.Unmarshal(p, &doc) == nil && doc.Node.Recovery.TCP == tcp && doc.Node.Recovery.UDP == udp && doc.Node.Recovery.Mask&(1<<tcp) != 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovery policy never became TCP=%d UDP=%d: %s", tcp, udp, p)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestMultipathRecoveryBlackhole(t *testing.T) {
	testMultipathRecoveryBlackhole(t, false)
}

func TestMultipathRecoveryHy2Blackhole(t *testing.T) {
	testMultipathRecoveryBlackhole(t, true)
}

func TestMultipathTrafficSavingRecoveryBlackhole(t *testing.T) {
	testMultipathRecoveryBlackhole(t, false, true)
}

func TestMultipathTrafficSavingRecoveryHy2Blackhole(t *testing.T) {
	testMultipathRecoveryBlackhole(t, true, true)
}

func testMultipathRecoveryBlackhole(t *testing.T, hy2 bool, saving ...bool) {
	for _, fast := range []bool{false, true} {
		for _, secondaryUDP := range []bool{false, true} {
			t.Run(fmt.Sprintf("tfo=%t/udp_leg1=%t", fast, secondaryUDP), func(t *testing.T) {
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer ln.Close()
				var accepts atomic.Int32
				var targets sync.WaitGroup
				go func() {
					for {
						c, e := ln.Accept()
						if e != nil {
							return
						}
						accepts.Add(1)
						targets.Add(1)
						go func() {
							defer targets.Done()
							defer c.Close()
							c.SetDeadline(time.Now().Add(25 * time.Second))
							io.Copy(c, c)
						}()
					}
				}()
				udp, err := net.ListenPacket("udp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer udp.Close()
				go func() {
					p := make([]byte, 65535)
					for {
						n, addr, e := udp.ReadFrom(p)
						if e != nil {
							return
						}
						udp.WriteTo(append([]byte(addr.String()+"|"), p[:n]...), addr)
					}
				}()
				instance, down, status := recoveryTestInstance(t, fast, secondaryUDP, false, hy2, len(saving) > 0 && saving[0])
				udpNormal := 0
				if secondaryUDP {
					udpNormal = 1
				}
				recoveryWaitStatus(t, status, 0, udpNormal)
				d, _ := instance.Outbound().Outbound("mp-out")
				c, err := d.DialContext(context.Background(), "tcp", M.SocksaddrFromNet(ln.Addr()))
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				pc, err := d.ListenPacket(context.Background(), M.SocksaddrFromNet(udp.LocalAddr()))
				if err != nil {
					t.Fatal(err)
				}
				defer pc.Close()
				exchange := func(label string) {
					t.Helper()
					c.SetDeadline(time.Now().Add(8 * time.Second))
					p := bytes.Repeat([]byte(label), 1024)
					if _, e := c.Write(p); e != nil {
						t.Fatal(e)
					}
					got := make([]byte, len(p))
					if _, e := io.ReadFull(c, got); e != nil || !bytes.Equal(p, got) {
						t.Fatalf("TCP %s %v", label, e)
					}
				}
				udpExchange := func() string {
					t.Helper()
					end := time.Now().Add(4 * time.Second)
					for {
						pc.SetDeadline(time.Now().Add(250 * time.Millisecond))
						pc.WriteTo([]byte("same-game"), udp.LocalAddr())
						p := make([]byte, 4096)
						n, _, e := pc.ReadFrom(p)
						if e == nil && strings.HasSuffix(string(p[:n]), "|same-game") {
							return strings.Split(string(p[:n]), "|")[0]
						}
						if time.Now().After(end) {
							t.Fatalf("UDP exchange failed: %v", e)
						}
					}
				}
				exchange("before")
				source := udpExchange()
				down.Store(true)
				// Write while the transport still looks connected. It must resume via
				// leg1 without application reconnect, then preserve TCP half-close.
				exchange("during-blackhole")
				recoveryWaitStatus(t, status, 1, 1)
				if other := udpExchange(); other != source {
					t.Fatalf("UDP source changed %s -> %s", source, other)
				}
				if accepts.Load() != 1 {
					t.Fatal("target TCP connection was recreated")
				}
				fresh, e := d.DialContext(context.Background(), "tcp", M.SocksaddrFromNet(ln.Addr()))
				if e != nil {
					t.Fatal("new session during outage", e)
				}
				fresh.SetDeadline(time.Now().Add(5 * time.Second))
				fresh.Write([]byte("new"))
				p := make([]byte, 3)
				if _, e = io.ReadFull(fresh, p); e != nil {
					t.Fatal(e)
				}
				fresh.Close()
				down.Store(false)
				recoveryWaitStatus(t, status, 0, udpNormal)
				exchange("after-return")
				if other := udpExchange(); other != source {
					t.Fatalf("UDP source changed on return %s -> %s", source, other)
				}
				if e := c.(interface{ CloseWrite() error }).CloseWrite(); e != nil {
					t.Fatal(e)
				}
				if _, e := io.ReadAll(c); e != nil {
					t.Fatal("half close", e)
				}
				c.Close()
				pc.Close()
				ln.Close()
				targets.Wait()
			})
		}
	}
}

func TestMultipathRecoveryStartsOnLeg1(t *testing.T) {
	instance, _, status := recoveryTestInstance(t, false, false, true)
	recoveryWaitStatus(t, status, 1, 1)
	d, _ := instance.Outbound().Outbound("mp-out")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, e := ln.Accept()
		if e == nil {
			defer c.Close()
			c.Write([]byte("hello"))
		}
	}()
	c, err := d.DialContext(context.Background(), "tcp", M.SocksaddrFromNet(ln.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	p := make([]byte, 5)
	if _, err = io.ReadFull(c, p); err != nil || string(p) != "hello" {
		t.Fatalf("server-first fallback %q %v", p, err)
	}
}

func TestMultipathRecoveryTFOCombinations(t *testing.T) {
	testMultipathRecoveryTFOCombinations(t, false)
}

func TestMultipathTrafficSavingRecoveryTFOCombinations(t *testing.T) {
	testMultipathRecoveryTFOCombinations(t, true)
}

func testMultipathRecoveryTFOCombinations(t *testing.T, saving bool) {
	for _, kind0 := range []string{multipathTestLegDirect, multipathTestLegProxy} {
		for _, kind1 := range []string{multipathTestLegDirect, multipathTestLegProxy} {
			for _, fast := range []bool{false, true} {
				for _, tfo0 := range []bool{false, true} {
					for _, tfo1 := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s-%t/%s-%t/mp-%t", kind0, tfo0, kind1, tfo1, fast), func(t *testing.T) {
							opts := multipathTFOTestOptions(fast, multipathTestLeg{tag: "leg0", kind: kind0, tfo: tfo0, proxyPort: otherPort}, multipathTestLeg{tag: "leg1", kind: kind1, tfo: tfo1, proxyPort: otherClientPort}, shadowaead.List[0], mkBase64(t, 16))
							mp := opts.Outbounds[len(opts.Outbounds)-1].Options.(*option.MultipathOutboundOptions)
							mp.FailoverEnabled = true
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

func TestMultipathRecoveryReceiveOnlyUDP(t *testing.T) {
	instance, down, status := recoveryTestInstance(t, false, false, false)
	recoveryWaitStatus(t, status, 0, 0)
	peer, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	d, _ := instance.Outbound().Outbound("mp-out")
	c, err := d.ListenPacket(context.Background(), M.SocksaddrFromNet(peer.LocalAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	peer.SetReadDeadline(time.Now().Add(5 * time.Second))
	c.WriteTo([]byte("register"), peer.LocalAddr())
	p := make([]byte, 65535)
	_, source, err := peer.ReadFrom(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, failed := range []bool{true, false} {
		down.Store(failed)
		path := 0
		if failed {
			path = 1
		}
		recoveryWaitStatus(t, status, path, path)
		// No more application uplink packets. The shared control channel must
		// move server-initiated datagrams, including a fragmented 8 KiB packet.
		payload := bytes.Repeat([]byte{byte(17 + path)}, 8192)
		peer.WriteTo(payload, source)
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, _, e := c.ReadFrom(p)
		if e != nil || !bytes.Equal(p[:n], payload) {
			t.Fatalf("one-way UDP leg%d n=%d: %v", path, n, e)
		}
	}
}

func TestMultipathRecoveryNewSessionAtFailure(t *testing.T) {
	for _, fast := range []bool{false, true} {
		t.Run(fmt.Sprint(fast), func(t *testing.T) {
			instance, down, status := recoveryTestInstance(t, fast, false, false)
			recoveryWaitStatus(t, status, 0, 0)
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			go func() {
				c, e := ln.Accept()
				if e == nil {
					defer c.Close()
					c.SetDeadline(time.Now().Add(8 * time.Second))
					io.Copy(c, c)
				}
			}()
			d, _ := instance.Outbound().Outbound("mp-out")
			down.Store(true) // Before the shared timeout, not after it.
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			c, err := d.DialContext(ctx, "tcp", M.SocksaddrFromNet(ln.Addr()))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(6 * time.Second))
			if _, err = c.Write([]byte("first")); err != nil {
				t.Fatal(err)
			}
			p := make([]byte, 5)
			if _, err = io.ReadFull(c, p); err != nil || string(p) != "first" {
				t.Fatalf("first write fallback %q: %v", p, err)
			}
		})
	}
}
