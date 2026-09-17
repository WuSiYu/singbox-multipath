package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-shadowsocks/shadowaead"
	M "github.com/sagernet/sing/common/metadata"
)

func TestMultipathNormalTCPHalfClose(t *testing.T) {
	for _, leg0Kind := range []string{multipathTestLegDirect, multipathTestLegProxy} {
		for _, leg1Kind := range []string{multipathTestLegDirect, multipathTestLegProxy} {
			for _, leg0TFO := range []bool{false, true} {
				for _, leg1TFO := range []bool{false, true} {
					for _, agg := range []bool{false, true} {
						for _, tfo := range []bool{false, true} {
							t.Run(fmt.Sprintf("leg0=%s-%t/leg1=%s-%t/aggregation=%t/tfo=%t", leg0Kind, leg0TFO, leg1Kind, leg1TFO, agg, tfo), func(t *testing.T) {
								ln, err := net.Listen("tcp", "127.0.0.1:0")
								if err != nil {
									t.Fatal(err)
								}
								defer ln.Close()
								response := make([]byte, 1<<20)
								for i := range response {
									response[i] = byte((i + i/1024*71) % 251)
								}
								serverDone := make(chan error, 1)
								go func() {
									c, e := ln.Accept()
									if e != nil {
										serverDone <- e
										return
									}
									defer c.Close()
									_ = c.SetDeadline(time.Now().Add(10 * time.Second))
									p, e := io.ReadAll(c)
									if e == nil && !bytes.Equal(p, []byte("complete request")) {
										e = fmt.Errorf("bad request")
									}
									if e == nil {
										_, e = c.Write(response)
									}
									serverDone <- e
								}()
								leg0 := multipathTestLeg{tag: "leg0", kind: leg0Kind, tfo: leg0TFO, proxyPort: otherPort}
								leg1 := multipathTestLeg{tag: "leg1", kind: leg1Kind, tfo: leg1TFO, proxyPort: otherClientPort}
								opts := multipathTFOTestOptions(tfo, leg0, leg1, shadowaead.List[0], mkBase64(t, 16))
								in := opts.Inbounds[1].Options.(*option.MultipathInboundOptions)
								out := opts.Outbounds[len(opts.Outbounds)-1].Options.(*option.MultipathOutboundOptions)
								_ = in
								out.Upload.AggregationEnabled = &agg
								out.Download.AggregationEnabled = &agg
								instance := startInstance(t, opts)
								time.Sleep(50 * time.Millisecond)
								d, found := instance.Outbound().Outbound("mp-out")
								if !found {
									t.Fatal("outbound missing")
								}
								c, err := d.DialContext(context.Background(), "tcp", M.SocksaddrFromNet(ln.Addr()))
								if err != nil {
									t.Fatal(err)
								}
								defer c.Close()
								_ = c.SetDeadline(time.Now().Add(10 * time.Second))
								if _, err = c.Write([]byte("complete request")); err != nil {
									t.Fatal(err)
								}
								cw, ok := c.(interface{ CloseWrite() error })
								if !ok {
									t.Fatalf("no half close on %T", c)
								}
								if err = cw.CloseWrite(); err != nil {
									t.Fatal(err)
								}
								got, err := io.ReadAll(c)
								if e := <-serverDone; e != nil {
									t.Fatalf("target error: %v received=%d", e, len(got))
								}
								if err != nil || !bytes.Equal(got, response) {
									t.Fatalf("normal TCP response truncated: got=%d want=%d err=%v", len(got), len(response), err)
								}
							})
						}
					}
				}
			}
		}
	}
}
