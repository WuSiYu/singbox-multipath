# sing-box

The universal proxy platform.

[![Packaging status](https://repology.org/badge/vertical-allrepos/sing-box.svg)](https://repology.org/project/sing-box/versions)

## Experimental multipath

This branch adds an experimental `multipath` inbound and outbound. It carries one
logical TCP byte stream over exactly two existing reliable outbounds. Traffic starts
on the preferred, stable low-latency leg (leg 0); the secondary leg (leg 1) joins the
data path after the configured traffic or queue threshold is reached. This is an
application-layer aggregation protocol, not kernel MPTCP. UDP is not aggregated and
is delegated to one selected child outbound.

The multipath protocol does not provide authentication or encryption by itself. The
aggregation listener should only be reachable through trusted or authenticated child
paths, such as a private WireGuard path and a Hysteria2 path.

### Client outbound example

The following example uses a system WireGuard interface for the preferred leg and an
already configured Hysteria2 outbound for the secondary leg:

```json
{
  "outbounds": [
    {
      "type": "direct",
      "tag": "wg-dedicated",
      "bind_interface": "wg1",
      "tcp_fast_open": true
    },
    {
      "type": "hysteria2",
      "tag": "hy2-public",
      "server": "hy2.example.com",
      "server_port": 443,
      "password": "change-me",
      "tls": {
        "enabled": true,
        "server_name": "hy2.example.com"
      }
    },
    {
      "type": "multipath",
      "tag": "mp-out",
      "outbounds": [
        "wg-dedicated",
        "hy2-public"
      ],
      "preferred": "wg-dedicated",
      "udp_outbound": "wg-dedicated",
      "server": "10.66.67.1",
      "server_port": 39000,
      "tcp_fast_open": true,
      "activation_threshold_mbps": 120,
      "activation_window": "1s",
      "chunk_size": 65536,
      "queue_frames": 256,
      "bandwidth_mbps": [
        160,
        700
      ]
    }
  ]
}
```

`tcp_fast_open` on the multipath outbound enables its early-write path: the
multipath hello and first data frame are emitted together. It does not enable TCP
Fast Open inside a child outbound. To place that first write in the TCP SYN, also
enable `tcp_fast_open` on the preferred child outbound and on the server's multipath
inbound, as shown in the examples. If the child transport does not support TCP Fast
Open, the combined early write still works but is sent after its connection is
established. When multipath `tcp_fast_open` is false or omitted, the multipath hello
is completed before the logical connection is returned.

### Server inbound example

```json
{
  "inbounds": [
    {
      "type": "multipath",
      "tag": "mp-in",
      "listen": "10.66.67.1",
      "listen_port": 39000,
      "tcp_fast_open": true,
      "activation_threshold_mbps": 120,
      "activation_window": "1s",
      "chunk_size": 65536,
      "queue_frames": 256,
      "bandwidth_mbps": [
        160,
        700
      ]
    }
  ]
}
```

Client and server scheduling parameters control traffic sent by that side and may be
tuned independently for asymmetric links. The client-requested `chunk_size` must not
exceed the server value.

### Outbound fields

| Field | Description |
| --- | --- |
| `outbounds` | Exactly two child outbound tags. Both children must support TCP. |
| `preferred` | Child used as leg 0 before aggregation activates. Defaults to the first entry in `outbounds`. |
| `udp_outbound` | Child used for UDP without aggregation. Defaults to `preferred` and must support UDP. |
| `server` / `server_port` | Address and port of the remote multipath inbound, reachable through both children. |
| `tcp_fast_open` | Enables the multipath early-write path. Also enable TCP Fast Open on the preferred child and server inbound for SYN data. Default: `false`. |
| `status_file` | Optional path for periodically written multipath runtime status JSON. |

### Shared tuning fields

| Field | Description |
| --- | --- |
| `activation_threshold_mbps` | Activates leg 1 when locally sent traffic reaches this average rate during `activation_window`. Defaults to `150` when this and `activation_after_bytes` are both unset. |
| `activation_after_bytes` | Optional total locally sent byte count that activates leg 1. It is an alternative trigger to the rate and queue triggers. |
| `activation_window` | Rate sampling window and sustained high-queue trigger duration. Default: `1s`. |
| `chunk_size` | Maximum payload per multipath data frame, from 1 KiB to 1 MiB. Default: 64 KiB. |
| `queue_frames` | Per-leg send queue capacity in frames, from 8 to 4096. `chunk_size * queue_frames` must not exceed 64 MiB. Default: 256. |
| `bandwidth_mbps` | Optional two-entry scheduling weights in leg 0/leg 1 order. Values express the expected relative capacity and are not rate limits. |
| `max_reorder_frames` | Server-only limit for buffered out-of-order frames. Default: 2048. |
| `max_reorder_bytes` | Limit for buffered out-of-order data. Default: 64 MiB; maximum: 512 MiB. |
| `leg1_replay_bytes` | Maximum retained send history used to recover data assigned to a stalled leg 1. Default: 64 MiB; maximum: 512 MiB. |
| `leg1_replay_timeout` | Time before unacknowledged leg 1 data is replayed on leg 0. Default: `5s`. |
| `handshake_timeout` | Timeout for a multipath leg handshake. Default: `10s`. |

The server inbound also accepts the standard sing-box listen fields, including
`listen`, `listen_port`, and `tcp_fast_open`.

## Documentation

https://sing-box.sagernet.org

## License

```
Copyright (C) 2022 by nekohasekai <contact-sagernet@sekai.icu>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

In addition, no derivative work may use the name or imply association
with this application without prior consent.
```
