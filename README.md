# sing-box

The universal proxy platform.

[![Packaging status](https://repology.org/badge/vertical-allrepos/sing-box.svg)](https://repology.org/project/sing-box/versions)

## Experimental multipath

This branch adds an experimental `multipath` inbound and outbound. It carries one
logical TCP byte stream over exactly two existing reliable outbounds. Traffic starts
on the preferred, stable low-latency leg (leg 0); the secondary leg (leg 1) joins the
data path after the configured traffic or queue threshold is reached. This is an
application-layer aggregation protocol, not kernel MPTCP. UDP is not aggregated.
By default it uses one child directly; optional failover relays it through the
same multipath server on either child while retaining the server's UDP socket.

The multipath protocol does not provide authentication or encryption by itself. The
aggregation listener should only be reachable through trusted or authenticated child
paths, such as a private WireGuard path and a Hysteria2 path.

Both endpoints must use multipath protocol **v12** (beta9). Older protocol versions are
rejected; there is no compatibility mode.

### Data path and leg roles

Leg 0 is the session anchor and preferred path. It creates the session, carries
cumulative acknowledgements and stream control, and carries all application data
before aggregation activates. This keeps connection setup and small transfers on the
configured low-latency path. With recovery disabled, losing leg 0 closes the logical
connection. Optional recovery permits session creation, control and data on leg 1.

Leg 1 is a capacity booster. It can attach while the connection is still using only
leg 0, but it does not carry application data until a local activation trigger fires
or optional failover takes over from leg 0.
Each direction makes that decision independently from its own accepted-byte count,
measured rate, and leg 0 backlog. After activation, the scheduler compares outstanding
path bytes divided by observed delivery rate. Receipt feedback comes from the far
multipath endpoint, so outstanding bytes include buffering inside a local proxy and
its remote transport. Completing a local socket write is not delivery confirmation.
There are no configured bandwidth weights or rate limits.

An unmeasured path starts with a bounded probe. Feedback establishes its observed
delivery rate and timing, not a guaranteed estimate of unused capacity. Once sampled,
the shared receive window, send-history budget and child write backpressure bound
assignment; there is no second per-path congestion window. Busy or stalled writers
do not block assignment to another eligible path. The preferred-only phase uses
normal child backpressure without the discovery-probe limit. Packet-level congestion
control, pacing, and retransmission remain in the child: this protocol does not
replace Hysteria2's congestion controller with an outer TCP one.

The client configures separate `upload` and `download` policies and sends both
during session establishment. Upload applies to the client sender and server
receiver; download applies to the server sender and client receiver. The server
accepts the exact session policy, subject to protocol validation and its own memory
budget. Joins and recovery reattachments must carry the same policy.

Each direction has an `aggregation_enabled` master switch. When false, new data
uses leg 0 except during optional failover. The other direction can still activate
leg 1. With `leg0_traffic_saving: true`, activation switches new data to leg 1
instead of using both legs. Before activation and while leg 1 is connecting,
data still uses leg 0. Healthy leg 1 writer backpressure or exhausted discovery
credit causes waiting, not spillover onto leg 0. A missing or stalled leg 1, or
local/peer memory pressure, permits leg 0 fallback; once eligible again, leg 1
resumes exclusive new-data transmission. Existing leg 0 assignments finish normally.
Reinjection and control traffic may still use leg 0, so this is not a zero-byte
guarantee. UDP preference and failover are separate from this switch.

With aggregation enabled, the following triggers are independent alternatives
(OR), evaluated separately for each connection and sending direction:

- Queue: `activation_on_queue` is enabled and leg 0 in-flight plus local unsent
  bytes stay at least 80% of `queue_frames * frame_size` for `activation_window`.
- Rate: `activation_threshold_mbps` is greater than zero and the average local
  ingress rate over `activation_window` reaches it.
- Bytes: `activation_after_bytes` is greater than zero and the local TX accepted-byte
  count reaches it. If `activation_after_bytes_min_mbps` is non-zero, this trigger
  also requires that average rate over a complete `activation_window`.

The minimum byte-trigger rate does not gate the queue or rate triggers. Disabling
all three triggers keeps local TX on leg 0; zero thresholds never imply immediate
activation. Once activated, aggregation does not automatically deactivate when
traffic drops. Explicit zero disables a numeric trigger. An omitted rate threshold
retains the default of 150 Mbps when the byte trigger is disabled, or zero when a
non-zero byte trigger is configured.

### Byte stream, receive window, and recovery

Both paths carry mappings into one 64-bit **byte** sequence space. Each path also
has a separate sequence space and incarnation ID. A receiver deduplicates overlapping
mappings and returns a cumulative Data ACK for the contiguous received prefix.
That ACK means the receiver owns the bytes, not that the application has read them.
DATA mappings are processed incrementally: an arriving prefix can be delivered and
acknowledged before the remaining payload of the same mapping reaches the receiver.
Only Data ACK releases the sender's connection-level history, which retains data
from **both** paths. An original or recovery writer holds an independent reference,
so acknowledgement cannot free a buffer while a blocked child still uses it.

The receiver advertises one monotonic byte-window right edge shared by both paths.
Application reads move the window; individual path receipts do not. Short writes
consume their actual byte length, not a whole frame credit. Local unsent capacity,
whole-path in-flight bytes, retained send history, and receive storage are separate
states. There are no idle-credit epochs, weight-based quotas, or separate leg 1
replay ownership rules.

Receive storage uses sparse 16 KiB pages, allocated only for arriving bytes. Under
memory pressure, a receiver may decline speculative data or prune wholly
unacknowledged out-of-order pages. It never discards Data-ACKed bytes waiting for the
application. Path receipts still report transport delivery; they do not release the
connection history. If a received mapping still covers the missing connection head,
the sender can reinject it from that history. Each admitted session has reserved
reader scratch, one head receive page, and a reusable primary TX buffer, so a missing
head is not dependent on leg 1 releasing speculative storage.

### Weak leg 1 and fallback

Each path has an independent writer. A blocked leg 1 write does not hold the state
lock, prevent leg 0 writes, or stop receive/control processing. Recovery reuses the
same immutable byte history, does not consume new connection-window space, and does
not need another payload allocation. Late originals are harmless duplicates.

Whole-path receipts drive delivery-rate and timing estimates. A leg without progress
is marked stale and pauses new assignments; retained mappings can be reinjected on
another available path. Receipt progress clears the stale state. A stall is not an
automatic disconnect/reconnect, and a hard secondary failure does not close the
logical stream. An individual leg receipt above a missing global byte is ordinary
reordering, not evidence that the missing byte was lost.

The adaptive no-progress interval is smoothed delivery RTT plus four times its
variation, at least 200 ms, initially one second before measurements are available.
An explicit `path_stall_timeout_min` supplies an additional lower bound. Buffering in
child transports is included in timing measurements. Control/window updates take
priority over DATA not yet submitted to the primary child; they cannot overtake an
already blocked child write or bytes already queued inside a reliable transport.

These mechanisms preserve a usable leg 0 through secondary stalls and failures.
They cannot guarantee the same latency as leg 0 alone after data have already been
assigned to a slow path: detecting and recovering an earlier missing byte takes
time and bandwidth. Aggregation also cannot exceed shared physical bottlenecks.

Payload storage, path/mapping metadata, reserved progress buffers, cache, and estimated
session overhead share one budget per multipath inbound or outbound. The default is
`min(512 MiB, available memory * 0.5)`. On Linux, available memory is the smaller of
`MemAvailable` and the remaining capacity of visible cgroup v1/v2 memory limits,
including ancestor groups. Explicit limits are not automatically reduced. New booster assignments and ordinary receive-window
growth pause at 7/8 and resume below 3/4; head progress remains reserved. Advertised
but unused window space is not an allocation. This budget is not process RSS and
does not include child TCP/QUIC buffers.

Checked-out cache entries release their old references immediately; sparse cache
indexes shrink, and large sender/path indexes are released when fully acknowledged.
Returning a buffer to the budget makes it reusable or collectible; it does not
force Go to return physical pages to the OS immediately. No periodic forced GC is
used. A high RSS alone is therefore not evidence of retained live buffers.

On a 1 GiB host, a conservative starting point is `"memory_limit": "256MB"`, with
headroom for child protocols, other processes and the OS. Multiple multipath
instances have separate budgets. Large QUIC windows can add substantial memory
outside them. If neither `GOMEMLIMIT` nor an existing runtime limit (including
sing-box's `debug.memory_limit`) is set, starting the first multipath instance
sets a process-wide Go **soft** limit to its current Go footprint plus 80% of
detected available memory. All multipath instances share it. Stopping the last
instance restores the previous setting unless it was subsequently overridden.
The startup log reports the effective value and its source. There is no periodic
memory polling or forced collection; normal Go GC uses this limit when needed.
Explicit `GOMEMLIMIT`, including `GOMEMLIMIT=off`, takes precedence. A soft limit
controls transient GC headroom, not RSS, and cannot fit a larger live working set
into RAM; see the [Go GC guide](https://go.dev/doc/gc-guide#Memory_limit).

Omitted receive/send-history ceilings are derived from the node budget: half of
its ordinary allocation region (7/16 of the total, capped at 512 MiB and at least
one frame). With a 512 MiB budget this is 224 MiB per direction. These are ceilings,
not allocations or per-session reservations; concurrent sessions still share the
same global allocator. Explicit byte limits remain hard caps. There is no frame-count receive limit; only the byte window and shared memory budget apply.

The byte-sequence, Data ACK, shared-window, reinjection, and DATA_FIN model follows
[RFC 8684](https://www.rfc-editor.org/rfc/rfc8684.html). The delivery-based scheduling
basis follows [Linux MPTCP](https://github.com/torvalds/linux/blob/587858367581b9c55c3690f4e63382ad622719d4/net/mptcp/protocol.c).
This is an independent implementation over reliable proxy streams, not MPTCP wire
compatibility, native subflow TCP congestion control, or an implementation of every
optional MPTCP path-management/security mechanism.

### Optional path failover

`failover_enabled` is a **client-only** option and defaults to `false`. Disabled
outbounds create no shared recovery probes or UDP relay sockets; their existing
aggregation and direct child UDP forwarding remain unchanged. The server always
listens on TCP and UDP and accepts both ordinary and recovery-enabled sessions.
It creates recovery groups only when requested by a client and has no
`failover_enabled` configuration field.

Enable the flag on the client outbound. The client also sets `failover_timeout`
(default `"5s"`) and `failback_delay` (default `"30s"`). Each
outbound maintains one TCP control connection and one native UDP association per
child, shared across all business connections. A path is healthy only while both
transports have fresh challenge replies from the multipath server. A full failure
timeout declares it unavailable; a delayed old reply does not establish recovery.
Checks normally run once per second. Detection and session reattachment add time
to the configured timeout; it is not a bound on application-visible interruption.

When leg 0 fails, existing TCP sessions retain their target connections, byte
sequence numbers, receive windows and unacknowledged send history. Data, cumulative
ACKs and FIN can use leg 1 without enabling aggregation. New sessions can also start
on leg 1. The client reconnects lost transports; the server does not redial the
target. A recovered leg 0 must remain healthy for the failback delay before normal
leg roles resume. One missed probe does not reset that period; a full failure
timeout does. If the fallback fails while the preferred path is available, the
stability hold is bypassed. Explicit application closure, server restart, or loss
of both paths beyond the recovery-group lease can still terminate a connection.

With recovery enabled, `udp_outbound` is the **preferred UDP leg**, independently
of TCP's leg 0 preference, and must name one of the two children. Both children must
support TCP and UDP. UDP uses the server relay from the first packet, so switching
paths does not replace the target-facing socket or its source port. Choosing leg 1
for UDP keeps it on leg 1 when leg 0 fails and recovers; only a failure of the UDP
preferred path triggers its own fallback. The same failure and return durations
apply. Client-selected path epochs also direct server replies, including one-way
application traffic; delayed messages cannot revert a newer selection.

UDP payloads remain unreliable datagrams, not UDP-over-TCP. The relay fragments
large packets into small outer datagrams and reassembles each independently, without
retransmission. Missing fragments expire after five seconds. Buffers use the shared
memory budget; pressure drops UDP packets rather than accumulating reliable queues.
UDP associations expire after five minutes without application packets. Closed session
IDs are retained for two minutes to reject delayed packets and joins; these records
also consume the shared budget. Group state expires after no control or UDP
traffic for `max(2 minutes, 4 * failover_timeout + failback_delay)`.

Recovery control heartbeats also verify TCP session ownership. The server rotates
through at most 64 IDs per heartbeat; the client reports which IDs it no longer
owns in the next request on that control connection. Only an explicit absence
releases a server session. Lost control connections discard their outstanding
query batch and retry through fresh queries, without accumulating a close queue.
Client ownership begins before sending the first hello, including fast open, and
ends when the logical core terminates. A connection still owned by the client is
not removed merely because both data legs are temporarily unavailable.

After the application calls full `Close`, buffered TX is allowed to drain, but
two minutes without cumulative Data ACK progress ends the remaining session.
This bounds abandoned FIN/ACK waits even if all control paths are unavailable.
It does not apply to `CloseWrite`, `CloseRead`, or an open idle connection. Normal
FIN acknowledgement completes the close immediately.

The server's listening port must be reachable over **both TCP and UDP** through both
children. The server's normal routing rules determine the final TCP and UDP exit.
The protocol adds no authentication or encryption; keep this listener on trusted
paths. Failover cannot prevent a game from disconnecting if its own timeout expires
during detection, or preserve a socket across server restart.

Client additions (independent of directional aggregation switches):

```json
{
  "failover_enabled": true,
  "failover_timeout": "5s",
  "failback_delay": "30s"
}
```

No server-side recovery option is required or accepted. All three fields above
are client-only; the client synchronizes its path selection and group lease to
the server. Remove `failover_enabled` from existing inbound configurations.

### Connection shutdown

DATA_FIN occupies one byte-sequence position at the end of each sending direction.
A cumulative Data ACK covers it only after all preceding bytes have been received. An application
close rejects further local I/O but drains accepted TX in the background; session-close
is sent only after the peer acknowledges that final sequence. Half-closing one
direction leaves the reverse direction usable, including through connection wrappers.

If FIN and all its preceding data have arrived, a subsequent session-close or control
leg transport failure preserves buffered RX until the application reads it. Receipt ACKs therefore
remain valid even when the application is slow. Session accounting is finalized after
that drain and buffer release. Errors, resets, and service shutdown can still abort
immediately; a local application close may discard its own unread RX. Normal draining
uses protocol confirmation rather than a fixed delay and remains subject to peer
backpressure and child transport failure. Service shutdown can interrupt a drain.

Application read/write deadlines also cover the initial fast-open write wait.
An application timeout does not reset the session; the accepted prefix remains
queued and the caller can resume with the unwritten suffix after clearing or
extending the deadline. An incomplete stream terminated without FIN reports an
error rather than a clean EOF, while local close interrupts pending application I/O.

### Runtime telemetry

When the client enables `status_file`, protocol v12 requests a compact sender-status
frame from the server on the control path (leg 0 normally, leg 1 during failover).
It reports the server-side downlink queues, replay and fallback counters,
write stalls, and memory pressure for the matching logical session. Status frames
are coalesced and do not consume data sequence numbers, replay space, or the payload
memory budget. The client marks remote status stale when updates stop rather than
interpreting missing telemetry as zero.

Active paths send low-rate PING/PONG probes independently of `status_file` for
observability. Data reinjection timing uses DATA receipt samples, not probe success alone.
An idle unused secondary does not receive per-flow probes just because it is attached.
Optional failover separately uses shared TCP/UDP health probes on both children.
Reported RTT is the effective application-layer round trip and
therefore includes transport and proxy queueing. Probe timeouts and reinjection/stall
counters describe multipath-visible events; they are not raw IP or UDP packet-loss
measurements. Traffic peaks are the highest one-second averages since process start,
while memory peaks are updated directly by the allocator.

Leg joins count successfully attached transports independently of local TX
activation. Joins, attempts and reported remote failures retain closed-session
totals; probe statistics cover active connections only. With failover enabled, attempts
count business TCP dials on each leg, excluding shared health connections. Without it,
leg 0 attempts count created logical connections. A lazy primary transport
can be attached before its deferred handshake finishes. Remote failure totals
include only events actually reported by the peer.

Leg events include `last_error_source`: `local_endpoint`, `remote_endpoint`,
`transport`, `shutdown`, or `unknown`. Closing a healthy logical connection marks
an application-endpoint shutdown; a preceding multipath failure keeps its original
source. Session-close frames carry this provenance to the peer on both legs.
Only close-related I/O errors inherit endpoint attribution; timeouts and protocol
errors remain visible. A missing close marker leaves the source unknown rather
than guessing from EOF, reset or QUIC cancellation text. The marker is diagnostic
only: it does not change FIN handling, scheduling, recovery or close timing.
Status schema 4 includes error provenance, directional policies and sender modes. Confirmed endpoint-close events do not
increment leg failure/event counters; other events, including unattributed and
harmless closures, retain their existing counting semantics. Protocol v12 requires
updating both endpoints.

Remote scheduler rate estimates are not one-second throughput or physical link
capacity. DATA-feedback RTT follows the selected data leg outward and leg0 for
the return feedback. Stall detection need not result in reinjection, and sender
backpressure duration is accumulated across connections, not a single pause.

At startup, each multipath inbound or outbound logs its resolved memory limit, high
and resume watermarks, and cache limit. Crossing the high watermark and recovering
below the resume watermark each emit one informational transition log.

### Client outbound example

The preferred leg can use a system WireGuard interface; the second leg can be an
existing Hysteria2 outbound. Upload stays on leg 0 in this example. Download switches
to leg 1 after any enabled trigger fires. Set `download.leg0_traffic_saving` to
`false` to aggregate instead.

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
      "tls": { "enabled": true, "server_name": "hy2.example.com" }
    },
    {
      "type": "multipath",
      "tag": "mp-out",
      "outbounds": ["wg-dedicated", "hy2-public"],
      "preferred": "wg-dedicated",
      "udp_outbound": "wg-dedicated",
      "server": "10.66.67.1",
      "server_port": 39000,
      "tcp_fast_open": true,
      "frame_size": "64KB",
      "upload": { "aggregation_enabled": false },
      "download": {
        "aggregation_enabled": true,
        "leg0_traffic_saving": true,
        "activation_on_queue": true,
        "activation_threshold_mbps": 120,
        "activation_after_bytes": "2MB",
        "activation_after_bytes_min_mbps": 120,
        "activation_window": "1s",
        "queue_frames": 256,
        "send_buffer_bytes": 0,
        "receive_window_bytes": 0,
        "path_stall_timeout_min": "0s"
      },
      "memory_limit": "512MB"
    }
  ]
}
```

Multipath `tcp_fast_open` combines the hello and first DATA write; it does not
enable TCP Fast Open inside a child. To carry that write in a TCP SYN, **also
enable the child's `tcp_fast_open`**, and enable it on the multipath server
listener. A child without TCP TFO still supports multipath early-write after
establishing its transport. With multipath TFO false or omitted, Dial waits for
the multipath hello response before returning the logical connection.

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
      "memory_limit": "512MB",
      "handshake_timeout": "10s"
    }
  ]
}
```

### Parameter ownership

The client is the multipath outbound; the server is the multipath inbound.
Policies apply per logical TCP connection. They do not tune child TCP/QUIC
buffers, congestion control, or UDP forwarding.

| Client policy | Sender | Receiver |
| --- | --- | --- |
| `upload` | Client | Server |
| `download` | Server | Client |

The client sends both policies and a common `frame_size` in the v12 hello.
The server validates them and confirms a digest; it does not substitute its own
directional defaults or negotiate a smaller frame. Both legs and all reattachments
must match. Invalid policies or insufficient server session-admission memory reject
the connection. The two hosts' `memory_limit` values remain independent and are
never overridden by the client.

An automatic send/receive ceiling is resolved on the host that owns that buffer.
For example, `upload.receive_window_bytes: 0` resolves against server memory;
`download.receive_window_bytes: 0` resolves against client memory. A zero on the
wire means automatic, not zero capacity. Explicit per-session ceilings never
override the owning host's shared memory budget.

### Client fields

| Field | Scope and peer interaction | Default / meaning | Accepted format examples |
| --- | --- | --- | --- |
| `outbounds` | Client-local tags define both-direction paths. | Exactly two TCP-capable children. | `["leg0", "leg1"]` |
| `preferred` | Assigns shared leg 0 role. | First child; initial/control/preferred path, including recovery. | `"leg0"` |
| `udp_outbound` | Client UDP preference; with failover, synchronized for server replies. | Preferred child. Without failover may name another outbound; with failover must name one of the two UDP-capable children. | `"leg0"`, `"leg1"` |
| `server`, `server_port` | Destination reached through both child outbounds. | Required aggregation listener. | `"10.66.67.1"`, `39000` |
| `tcp_fast_open` | Client logical-connection setup. | False; early-write when true. Also enable child TFO for TCP SYN data. Server listener TFO is a separate socket option. | `true`, `false` |
| `frame_size` | Client-selected maximum DATA payload, shared by both TX/RX directions and confirmed by server. | 64 KiB; omitted/0 uses default, explicit nonzero range 1 KiB–1 MiB. Payload excludes headers; frames can be smaller and are read incrementally. | `65536`, `"64KB"`, `"16 KB"` |
| `upload`, `download` | Client sends immutable directional policies to server. | Both default to aggregation enabled, traffic-saving disabled. Fields below. | `{"aggregation_enabled": false}` |
| `status_file` | Client-local output path; hello requests peer sender telemetry. | Disabled when empty. One-second JSON status including policies, effective buffer ceilings and directional modes. | `"/var/run/multipath.json"` |
| `failover_enabled` | Client-only shared TCP/UDP recovery; server always supports it. | False; no shared recovery probes/UDP relay associations when off. Independent of activation and traffic-saving. | `true`, `false` |
| `failover_timeout` | Client path failure detection; sent to recovery group. | 5 seconds; requires failover enabled. | `"5s"`, `"10s"` |
| `failback_delay` | Client preferred-path stability hold; synchronized with server. | 30 seconds; requires failover enabled. It restores normal policy, not forced leg 0 DATA in an activated saving direction. | `"30s"`, `"1m"` |

### Fields inside upload and download

All these fields are set **only on the client**, then applied on the appropriate
host. Sender means client for upload and server for download. Receiver means the
opposite host. Limits are per logical connection and direction.

| Field | Application | Default / meaning | Accepted format examples |
| --- | --- | --- | --- |
| `aggregation_enabled` | Sender activation master switch. | True; false keeps new DATA on leg 0 except optional failover. | `true`, `false` |
| `leg0_traffic_saving` | Sender path-selection policy. | False; true switches to leg 1 instead of aggregating after activation. Has no activation effect by itself. | `true`, `false` |
| `activation_on_queue` | Sender **condition 1**. | True; leg 0 in-flight plus unsent bytes stay at least 80% of `queue_frames * frame_size` over the window. | `true`, `false` |
| `activation_threshold_mbps` | Sender **condition 2**. | Average accepted application rate. Omitted: 150 Mbps, or 0 when byte trigger is enabled. Explicit 0 disables. | `0`, `120` |
| `activation_after_bytes` | Sender **condition 3**. | Cumulative accepted application bytes; 0 or omitted disables. Subject to the following rate gate only. | `0`, `2097152`, `"2MB"` |
| `activation_after_bytes_min_mbps` | Extra AND gate within condition 3. | 0; otherwise requires minimum average rate over a complete activation window as well as byte count. Does not gate conditions 1 or 2. | `0`, `5`, `120` |
| `activation_window` | Sender queue/rate sampling interval. | 1 second; omitted or zero uses default. | `"1s"`, `"500ms"` |
| `queue_frames` | Sender unsent capacity, in frame-size units. | 256; omitted/0 defaults, explicit range 8–4096. Product with frame_size ≤64 MiB. Not an in-flight or receive-frame count. This storage is part of send_buffer_bytes. | `64`, `256` |
| `send_buffer_bytes` | Sender connection history, **both paths**, including unsent bytes. | Automatic on sender host; released only by cumulative Data ACK. Explicit range frame_size–512 MiB. | `0`, `67108864`, `"64MB"` |
| `receive_window_bytes` | Receiver byte-window span constraining sender TX. | Automatic on receiver host; includes unread in-order bytes. Explicit range frame_size–512 MiB. No independent frame-count limit. | `0`, `134217728`, `"128MB"` |
| `path_stall_timeout_min` | Sender no-progress detection floor on either leg. | Adaptive when omitted/zero; nonzero adds a lower bound. Range 100 ms–5 minutes. Not a fixed retransmission interval or a disconnect deadline. | `"0s"`, `"500ms"`, `"2s"` |

Conditions **1 OR 2 OR 3** activate leg 1; disabling all three prevents activation.
The master switch overrides all conditions. Once activated, the direction remains
activated for the connection lifetime; no low-rate switchback is performed.

### Host-local fields and server listener

| Field | Scope and peer interaction | Default / meaning | Accepted format examples |
| --- | --- | --- | --- |
| `memory_limit` | Local inbound/outbound shared budget across TCP and recovery UDP sessions. Never negotiated or overridden by peer policy. | `min(512 MiB, available memory * 0.5)`; omitted/0 automatic. Linux uses the smaller of MemAvailable and visible cgroup remaining capacity. Does not cap child transport buffers or process RSS. | `0`, `268435456`, `"256MB"` |
| `handshake_timeout` | Local leg-handshake deadline, independent on each host. | 10 seconds; omitted/zero default. | `"10s"`, `"5s"` |
| `listen`, `listen_port` | Server-local TCP and UDP listener. | Standard sing-box listen fields; reachable through both children when failover is used. | `"10.66.67.1"`, `39000` |
| Server `tcp_fast_open` | Server TCP socket option, not a directional policy. | Standard listener option. | `true`, `false` |

Memory strings use binary units: `"2MB"` and `"2 MB"` both mean 2,097,152 bytes.
Suffixes are case-insensitive `B`, `K`/`KB`, `M`/`MB`, through `E`/`EB`.
The memory parser does not accept `KiB`/`MiB` suffixes or fractional quantities.
Automatic buffer limits are ceilings, not preallocations. A smaller shared budget
can apply backpressure before any one connection reaches its ceiling.

### Beta8 field removal

Beta8 recognizes the following **old flat fields only to warn and ignore them**:
`aggregation_enabled`, all five `activation_*` fields, `queue_frames`,
`chunk_size`, `max_reorder_frames`, `max_reorder_bytes`,
`leg1_replay_bytes`, `leg1_replay_timeout`, and `bandwidth_mbps`.
The old names for frame/reorder/replay/weights are also ignored with a warning if
placed inside a direction object. They are not migrated, used as defaults, or
allowed to override new fields. Even an invalid legacy value is ignored.

Reconfigure both directions on the client. Remove all directional tuning from
the server. Replace chunk_size with frame_size, leg1_replay_bytes with
send_buffer_bytes, max_reorder_bytes with receive_window_bytes, and
leg1_replay_timeout with path_stall_timeout_min. There is no replacement frame-count
receive limit and no `receive_window_frames` option. Unknown fields and invalid
new-field values are errors. Upgrade both endpoints together; old wire versions
are rejected immediately.

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
