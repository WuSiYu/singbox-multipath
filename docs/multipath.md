# Multipath mechanisms

[中文](multipath.zh-CN.md) · [Configuration reference](../README.md#english)

This document describes the implemented Multipath data path, session lifecycle and observability. The README contains deployment examples and the complete configuration reference.

## Scope and relationship to MPTCP

Multipath combines exactly two reliable child streams between a client outbound and a server inbound. The server maintains one target-facing TCP connection for each logical stream. Child nodes forward ordinary traffic to the aggregation listener and do not need to understand Multipath.

MPTCP presents one ordered byte stream over multiple TCP subflows, with a connection-level sequence space, Data ACK and cross-path data mappings. Multipath adopts those connection-level ideas, including reinjection and an ordered FIN, but carries its own framed protocol over proxy streams rather than TCP options. See [RFC 8684](https://www.rfc-editor.org/rfc/rfc8684.html).

Native TCP/QUIC congestion control and retransmission remain inside the children. Multipath does not implement MPTCP's authenticated MP_JOIN, address discovery, TCP fallback or coupled subflow congestion control. The scheduling reference is the [Linux MPTCP implementation at a pinned commit](https://github.com/torvalds/linux/blob/587858367581b9c55c3690f4e63382ad622719d4/net/mptcp/protocol.c), not a promise to track every future kernel change.

The listener has no built-in authentication or encryption. Session IDs and policy digests bind protocol state; they do not authenticate a peer. Use trusted paths or authenticated proxies and restrict listener access. Current endpoints require protocol v12; older wire versions are rejected.

## Data path and leg roles

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

## Byte stream, receive window, and recovery

Both paths carry mappings into one 64-bit **byte** sequence space. Each path also
has a separate sequence space and incarnation ID. A receiver deduplicates overlapping
mappings and returns a cumulative Data ACK for the contiguous received prefix.
That ACK means the receiver owns the bytes, not that the application has read them.
DATA mappings are processed incrementally: an arriving prefix can be delivered and
acknowledged before the remaining payload of the same mapping reaches the receiver.
The wire format still uses DATA frames. `frame_size` caps the payload of one frame;
it is not a fixed-size record or an application read unit. Missing earlier bytes
still block ordered delivery of later bytes: incremental processing does not
eliminate connection-level head-of-line blocking.
Only Data ACK releases the sender's connection-level history, which retains data
from **both** paths. An original or recovery writer holds an independent reference,
so acknowledgement cannot free a buffer while a blocked child still uses it.

The receiver advertises one monotonic byte-window right edge shared by both paths.
Application reads move the window; individual path receipts do not. Short writes
consume their actual byte length, not a whole frame credit. Local unsent capacity,
whole-path in-flight bytes, retained send history, and receive storage are separate
states. There are no idle-credit epochs, weight-based quotas, or separate leg 1
replay ownership rules.

| State | Meaning |
| --- | --- |
| Local unsent data | Accepted bytes not yet assigned for transmission; bounded by `queue_frames * frame_size` and included in send history. |
| Path in-flight data | Bytes assigned to one leg but not yet covered by a whole-path receipt, including child buffers. |
| Connection send history | Bytes not yet covered by cumulative Data ACK, including both paths and unsent data; bounded by `send_buffer_bytes`. |
| Receive storage | Arrived out-of-order data and unread in-order data, subject to the byte window and shared memory budget. |

`queue_frames` therefore does not define a second copy of the retransmission
buffer, and `send_buffer_bytes` is not exclusive to leg1.

Receive storage uses sparse 16 KiB pages, allocated only for arriving bytes. Under
memory pressure, a receiver may decline speculative data or prune wholly
unacknowledged out-of-order pages. It never discards Data-ACKed bytes waiting for the
application. Path receipts still report transport delivery; they do not release the
connection history. If a received mapping still covers the missing connection head,
the sender can reinject it from that history. Each admitted session has reserved
reader scratch, one head receive page, and a reusable primary TX buffer, so a missing
head is not dependent on leg 1 releasing speculative storage.

## Weak leg 1 and fallback

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

## Memory and backpressure

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

## Optional path failover

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
At startup, either confirmed healthy path can carry traffic immediately. The
preferred path is selected as soon as it first becomes healthy, without a failback
hold. Once that path has been healthy and then failed, subsequent returns require
the configured stability period. This applies separately to TCP and UDP preference.

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
Pending child handshakes are interrupted when their context is canceled. Session
cleanup waits for secondary-join and recovery-rejoin workers before returning
their memory reservation.

The server's listening port must be reachable over **both TCP and UDP** through both
children. The server's normal routing rules determine the final TCP and UDP exit.
The protocol adds no authentication or encryption; keep this listener on trusted
paths. Failover cannot prevent a game from disconnecting if its own timeout expires
during detection, or preserve a socket across server restart.

## Connection shutdown

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

## Runtime telemetry

When the client enables `status_file`, protocol v12 requests a compact sender-status
frame from the server on the control path (leg 0 normally, leg 1 during failover).
It reports the server-side downlink queues, replay and fallback counters,
write stalls, and memory pressure for the matching logical session. Status frames
are coalesced and do not consume data sequence numbers, replay space, or the payload
memory budget. The client marks remote status stale when updates stop rather than
interpreting missing telemetry as zero.
Telemetry still has metadata and transport overhead; it is not cost-free.

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

Current aggregate and traffic-saving modes require an attached leg 1 on the same
client connection. A fresh remote mode cannot override local path absence; the
direction becomes unknown and an activated connection with no booster is reported
as degraded. An attached idle connection remains eligible regardless of its current
transfer rate. Remote modes older than three seconds are unknown.

Remote scheduler rate estimates are not one-second throughput or physical link
capacity. DATA-feedback RTT follows the selected data leg outward and leg0 for
the return feedback. Stall detection need not result in reinjection, and sender
backpressure duration is accumulated across connections, not a single pause.

At startup, each multipath inbound or outbound logs its resolved memory limit, high
and resume watermarks, and cache limit. Crossing the high watermark and recovering
below the resume watermark each emit one informational transition log.

## Source map

| Area | Source |
| --- | --- |
| Client-selected directional policies and validation | [policy.go](../protocol/multipath/policy.go), [option/multipath.go](../option/multipath.go) |
| Assignment, delivery feedback and reinjection | [scheduler.go](../protocol/multipath/scheduler.go), [stream/path.go](../protocol/multipath/stream/path.go) |
| Connection send history and receive storage | [stream/send.go](../protocol/multipath/stream/send.go), [stream/receive.go](../protocol/multipath/stream/receive.go) |
| Shared budget, cgroup detection and Go limit | [memory.go](../protocol/multipath/memory.go), [memory_available_linux.go](../protocol/multipath/memory_available_linux.go), [memory_runtime.go](../protocol/multipath/memory_runtime.go) |
| Shared recovery health and session ownership | [recovery_client.go](../protocol/multipath/recovery_client.go), [recovery_policy.go](../protocol/multipath/recovery_policy.go), [recovery_sessions.go](../protocol/multipath/recovery_sessions.go) |
| Shutdown and half-close | [lifecycle.go](../protocol/multipath/lifecycle.go), [logical_conn.go](../protocol/multipath/logical_conn.go) |
| Remote telemetry and client status | [telemetry.go](../protocol/multipath/telemetry.go), [status.go](../protocol/multipath/status.go) |

The [beta5 design](development/multipath-beta5.md) and [validation record](development/multipath-beta5-validation.md) are historical development documents, not the current configuration reference.
