package main

import (
	"fmt"
	"testing"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-shadowsocks/shadowaead"
)

func TestMultipathDirectionalReceiveWindows(t *testing.T) {
	for _, limits := range [][2]uint32{{4096, 8192}, {8192, 4096}} {
		for _, fastOpen := range []bool{false, true} {
			name := fmt.Sprintf("client=%d/server=%d/tfo=%t", limits[0], limits[1], fastOpen)
			t.Run(name, func(t *testing.T) {
				leg0 := multipathTestLeg{tag: "leg0", kind: multipathTestLegDirect, tfo: fastOpen}
				leg1 := multipathTestLeg{tag: "leg1", kind: multipathTestLegProxy, tfo: fastOpen, proxyPort: otherClientPort}
				options := multipathTFOTestOptions(fastOpen, leg0, leg1, shadowaead.List[0], mkBase64(t, 16))
				options.Outbounds[len(options.Outbounds)-1].Options.(*option.MultipathOutboundOptions).Upload.ReceiveWindowBytes = multipathMemoryBytes(fmt.Sprint(uint64(limits[1]) * 16384))
				options.Outbounds[len(options.Outbounds)-1].Options.(*option.MultipathOutboundOptions).Download.ReceiveWindowBytes = multipathMemoryBytes(fmt.Sprint(uint64(limits[0]) * 16384))
				// Both windows are selected by the client, each applying to the receiver of its direction.
				startInstance(t, options)
				testTCP(t, clientPort, testPort)
			})
		}
	}
}
