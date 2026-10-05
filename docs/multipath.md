# Multipath mechanisms

[中文](multipath.zh-CN.md) · [Configuration reference](../README.md#english)

This document describes the Multipath data path, scheduling, memory, session lifecycle and observability of protocol v13 (beta10). The README contains deployment examples and the complete configuration reference.

## Scope and relationship to MPTCP

Multipath combines exactly two reliable child streams between a client outbound and a server inbound. The server maintains one target-facing TCP connection for each logical stream. Intermediate nodes forward ordinary traffic to the aggregation listener and do not need to understand Multipath.

MPTCP presents one ordered byte stream over multiple TCP subflows, with a connection-level sequence space, Data ACK and cross-path data mappings. Multipath adopts those connection-level ideas, including an msk-level send buffer, earliest-completion-first (ECF) scheduling, opportunistic reinjection with penalties and an ordered FIN, but carries its own framed protocol over proxy streams rather than TCP options and does not interoperate with the MPTCP wire protocol. See [RFC 8684](https://www.rfc-editor.org/rfc/rfc8684.html).

Packet-level congestion control, pacing and retransmission remain inside the children, including the congestion control used by Hysteria2. Multipath does not implement MPTCP address discovery, TCP fallback or coupled subflow congestion control. The scheduling reference is the [Linux MPTCP implementation at a pinned commit](https://github.com/torvalds/linux/blob/587858367581b9c55c3690f4e63382ad622719d4/net/mptcp/protocol.c), not a promise to track every future kernel change.

## Authentication and access control

Multipath does not encrypt data; the child protocols or private links do. An unprotected listener lets anyone who can reach it use the server to connect to arbitrary targets.

- **`psk`**: with the same pre-shared key on both endpoints, every hello carries an HMAC-SHA256 tag over the session ID, policy, destination, a timestamp and a random nonce. The server rejects a wrong tag, a clock skew above two minutes or a replayed nonce. The PSK protects session setup; it does not encrypt the data that follows.
- **`allowed_ips`** (server only): accept child connections only from these prefixes, judged by the source address the server sees.
- A server with neither `psk` nor `allowed_ips` listening on a non-private address logs a warning at startup.

Both endpoints must run protocol v13. On a version mismatch the server answers with an explicit rejection carrying its version, and the client logs the reason instead of failing obscurely. Other rejections (authentication, parameter mismatch, closed session) also carry a reason code.

## Sessions and legs

The two legs are equals for control: WINDOW feedback, DATA_FIN, session close, reset and sender status are valid on either ready leg, and **no leg is essential to a session**. Leg 0 remains the preferred path for session creation and for application data before activation.

- **Creation**: the client creates the session through the preferred leg; if that leg cannot reach the server within half of `handshake_timeout`, the other leg creates it. Each leg then has its own connection manager.
- **Redial**: a lost leg is redialed with exponential backoff (1 s doubling to 30 s, reset after 30 s of stable attachment) and rejoins the same session; the server does not dial the target again.
- **Failed legs**: a stalled leg takes no new data; a leg without any progress for 15 s is closed for redial while the other leg is usable. A sole leg is never closed this way.
- **Losing leg 0**: new data move to leg 1, and feedback, FIN and close proceed on leg 1 without failover or aggregation. On the testbed a two-second leg 0 outage keeps about 950 Mbps on leg 1.
- **No legs at all**: without failover, a session ends after both legs have been absent for `max(30 s, 3 × handshake_timeout)`. With failover, the recovery-group lease and ownership checks decide (see below).
- **Tombstones**: the server remembers closed session IDs for two minutes and rejects late joins and repeated creation, so one ID never dials its target twice.

WINDOW feedback goes on the least blocked leg, and the other leg receives a copy at least every 20 ms, or immediately when the chosen leg's writer has been blocked for `max(10 ms, SRTT/2)`. Copies carry a sequence number: monotonic fields (cumulative ACK, window edge, path receipts) merge by maximum, while flags follow only the newest copy. DATA_FIN is repeated on the other leg when its leg disappears or it stays unacknowledged for an RTO.

## Activation

Upload and download are configured and activated independently. `aggregation_enabled` is the master switch. When it is on, the following triggers are evaluated per connection and per sending direction, and any one of them activates:

1. **Queue trigger** (`activation_on_queue`, on by default): unsent bytes stay at or above half of the current unsent limit for `activation_window` (default 200 ms), **and** leg 0's delivery rate has stopped growing (less than 25% gain over two round trips, as BBR detects a full pipe). The first condition means the application outpaces leg 0; the second means leg 0 has left slow start and really is the bottleneck.
2. **Rate trigger** (`activation_threshold_mbps`, default 0 = off): the average rate of accepted application data in the window reaches the threshold.
3. **Byte trigger** (`activation_after_bytes`, off by default): accepted application bytes reach the threshold; a non-zero `activation_after_bytes_min_mbps` additionally requires that average rate over a complete window.

The plateau condition keeps mid-size transfers on high-RTT paths from pulling in leg 1 while leg 0 is still in slow start. Leg 1's fresh child connection would then be in slow start too, and the early bytes it carries often arrive after leg 0 has delivered everything else. On low-RTT paths leg 0 leaves slow start within tens of milliseconds, so `activation_window` dominates.

With every trigger disabled, nothing activates; a numeric zero does not mean immediate activation. An activated connection stays activated. For the pre-beta10 behavior, set `activation_threshold_mbps: 150` and `activation_window: "1s"` explicitly.

## Scheduling

### Earliest completion first

After activation each new segment goes to the path expected to deliver it first. A path's expected completion is

`(bytes outstanding on the path + segment length) / delivery rate + minimum RTT / 2`

Delivery rate and RTT come from path receipts sent by the far Multipath endpoint, so outstanding bytes include buffering inside a local proxy and its remote transport; finishing a local socket `Write` is not delivery. There are no configured bandwidth weights or rate limits.

- While the application is backlogged (its writes block, or blocked within the last 10 ms), a slower idle path may take a segment that completes within 1.5 times the best path's completion, so that both paths stay busy.
- Once the application has finished (DATA_FIN queued) or has nothing more for now, the rule is strict ECF: a slower path takes a segment only if it delivers it before the best path could deliver everything pending, so the tail of a transfer does not end up on a slow path.
- A path without a rate sample borrows the best measured rate and the longest measured delay, and holds at most four frames until its first sample.
- While the application is backlogged, a slower path may always keep twice what it delivered in its last propagation round trip (at least two frames) in flight, regardless of its estimate. Otherwise a late-joining or recovering path would have its estimate held down by the scheduler's own restraint and never show its capacity. This grows its in-flight data per round trip like the child's own slow start, up to about twice its bandwidth-delay product.
- A path idle for `max(2 × SRTT, 100 ms)` is probed with an optimistic rate, and the first receipt after an idle period starts a new rate sample instead of averaging over the gap.

### Repair and reinjection

Every resend borrows from the one connection-level send history: it consumes no new sequence space or receive window and copies no payload. A late original arrives as a duplicate range and is never delivered twice.

- **Receiver drops**: a receiver that refused data for lack of memory reports up to four dropped ranges in WINDOW feedback (never merged across gaps). The sender repairs a range at most once per RTO and spends at most a quarter of the delivery rate per RTT on repairs.
- **Failed paths**: when a leg is replaced (new generation) or stalls, its unacknowledged mappings are resent on another usable path.
- **Opportunistic reinjection with penalty**: when new data are blocked by the receive window or the send history, and the path holding the connection head has delivered nothing for one of its RTTs (and at least twice the fast path's RTT), an idle path resends the data near the head in order, and the stuck path takes no new data for one of its RTTs.
- **Tail reinjection**: when nothing new is left to send and data near the head are still queued on a slower or still-starting path that would deliver them later than an idle faster path could now, the faster path sends another copy; whichever arrives first is used. This covers a child connection in slow start holding data for several round trips.

### Stall detection

The no-progress timeout is the smoothed delivery RTT plus four times its variation, at least 200 ms, or 1 s before any sample. `path_stall_timeout_min` can only raise that floor; it is not a fixed retransmission period. A stalled path takes no new data until it makes progress again; a stall alone does not require a reconnect.

## Byte stream, send history and receive window

Data on both paths map into one 64-bit connection-level byte sequence; each path also has its own sequence and incarnation. The receiver handles duplicate and overlapping mappings and returns a cumulative Data ACK for the contiguous prefix. That means the receiver owns those bytes, not that the target application has read them.

`frame_size` is the largest DATA payload, not a fixed record size. The receiver processes frames incrementally: a contiguous prefix is delivered and acknowledged as soon as it arrives. Missing earlier bytes still hold later ones back; byte-level processing does not remove head-of-line blocking from an ordered stream.

| State | Meaning and limit |
| --- | --- |
| Unsent data | Bytes accepted by Multipath but not yet assigned to a path. Limited to about 10 ms of the carrying paths' delivery rate, at least 1 MiB (or four frames), at most `queue_frames × frame_size`, like MPTCP's `notsent_lowat`. |
| Path in flight | Bytes assigned to a leg without a complete path receipt yet, including child-internal buffering. |
| Send history | Every byte not yet covered by the cumulative Data ACK, unsent bytes included, like MPTCP's msk send buffer. The target is twice the carrying paths' bandwidth-delay product plus the unsent limit, at least 4 MiB. It grows 25% per RTT while history rather than the network limits sending and shrinks back gradually when demand falls; it is capped by `send_buffer_bytes` and by the session's fair share of the transmit region. |
| Receive window | One connection-level byte window advertised for both paths; its right edge only moves forward. It is 256 KiB before the first advertisement and then the session's fair share (see below), at most `receive_window_bytes`. Application reads move it forward. |
| Receive storage | Out-of-order data and in-order data not yet read by the application, allocated lazily in pooled 16 KiB pages. |

`queue_frames`, `send_buffer_bytes` and `receive_window_bytes` are **ceilings**, not capacities or preallocations; the working values follow path rates, RTTs and memory shares. The defaults are the recommended values and rarely need tuning.

Application reads and writes copy directly between the application's buffers and the receive pages or send buffers, without relay pipes or relay goroutines, and each DATA frame's header and payload go to the child in one write.

## Memory and backpressure

Each Multipath inbound or outbound has one host-local budget covering send payload, receive pages, metadata, caches and fixed session overhead. The default is `min(512 MiB, available memory × 0.5)`; on Linux the available memory is the smaller of `MemAvailable` and the visible cgroup v1/v2 headroom. An explicit budget is never shrunk automatically.

One sixteenth of the budget is an emergency margin; the rest is a shared pool with three accounts: transmit payload (tx), receive pages (rx) and other (session reservations, UDP reassembly and so on).

- Transmit and receive borrow whatever the other does not use, but always leave it a reserve: half of the pool while the other direction has active sessions, otherwise an eighth, plus a 1/16 band so one side filling the pool never prevents the other from starting.
- Send history gets a fair share of the transmit region divided by active senders; receive windows get three quarters of the receive region divided by active receivers (at least 256 KiB). The remaining quarter absorbs page overhead and arrivals at the window edge. A session counts as active while it moved data within the last second.
- When receive pages run out, data within 16 pages of the next expected byte are still admitted and evict the farthest unacknowledged page; refused or evicted ranges are reported to the sender for repair.
- After refusing data, or with room for less than two frames, the receiver sets a "full" flag in its feedback: the sender pauses new data (keeping at most two frames in flight) and only repairs holes until the receiver reports room again.
- Bytes already covered by Data ACK and waiting for the application are never discarded.

"Pressure" only means the emergency margin is in use. It is logged and reported, but it **never disables leg 1, freezes receive windows or changes any session's path selection**.

Every admitted session reserves reader scratch, one head receive page and one reusable transmit buffer, so head-of-line progress never depends on another session releasing memory first. Returning a buffer to the budget makes it reusable or collectable; Go does not necessarily return physical pages to the operating system immediately.

On a 1 GiB host, start with `"memory_limit": "256MB"` to leave room for child protocols, other processes and the kernel. Separate Multipath instances have separate budgets, and large QUIC windows use memory outside the budget.

Without `GOMEMLIMIT` or an existing runtime memory limit (including sing-box `debug.memory_limit`), the first Multipath instance sets the process Go soft limit to "current Go memory + 80% of detected available memory". All instances share it and the previous setting is restored when the last one stops. An explicit `GOMEMLIMIT`, including `off`, takes precedence. It is not a hard RSS limit; see the [Go GC guide](https://go.dev/doc/gc-guide#Memory_limit).

## Traffic-saving mode

With `leg0_traffic_saving`, new data after activation go exclusively to leg 1 instead of being aggregated. Before activation, and while leg 1 is not ready, leg 0 is used. Write backpressure on a healthy leg 1 makes data wait rather than spill onto leg 0; a missing or stalled leg 1 lets leg 0 take over until leg 1 recovers. While leg 1 is healthy, tail reinjection does not use leg 0 either. Data already assigned to leg 0 complete normally, and control frames and repairs after a failure may still use leg 0, so the mode does not guarantee zero leg 0 traffic.

## Optional path failover

`failover_enabled` is a client-only option and defaults to false. Without it, a session already survives on either leg (see above); failover adds shared health checks, path selection for new sessions, a UDP relay with UDP path switching, and session retention across long outages of both legs. The server always listens on TCP and UDP and creates a recovery group only when the client asks for one.

When enabled, each outbound keeps one shared TCP control connection and one native UDP association per child for all business connections. A path is healthy only with fresh TCP and UDP challenge responses; it becomes unavailable after a full `failover_timeout` (default 5 s) without a valid response. Probes normally run once per second, and detection plus session reconnection add time, so the timeout is not an upper bound on interruption.

The preferred path is used as soon as it is first confirmed healthy. After it has failed once, a recovering preferred path must stay healthy before it returns to its normal role: 3 s at first, doubling with repeated failures up to `failback_delay` (default 30 s). A single missed probe does not restart the hold; a full failure timeout does. If the backup also fails, a healthy preferred path is used immediately. TCP and UDP each follow their own preferred path.

With failover, `udp_outbound` is the UDP preferred path, independent of the TCP leg 0, and must be one of the two children. UDP goes through the server relay from the first packet, and switching paths keeps the target-facing socket and source port. The server answers according to the path epoch synchronized by the client, and late old messages cannot undo a newer selection.

UDP keeps unreliable datagram semantics and is not carried over TCP. The relay splits large datagrams into smaller outer datagrams and reassembles each independently without Multipath-level retransmission; incomplete datagrams expire after five seconds. Reassembly buffers count against the shared budget, and pressure drops UDP datagrams instead of building a reliable queue. UDP associations expire after five minutes without application datagrams. A recovery group expires after `max(2 minutes, 4 × failover_timeout + failback_delay)` without control or UDP traffic.

Recovery heartbeats also confirm TCP session ownership. Each round the server asks about at most 64 IDs, cycling through active sessions; the client reports IDs it no longer holds, and only an explicit "gone" releases a server session. The client registers ownership before the first hello (including fast open) and unregisters it when the logical connection ends, so a session the client still holds is not reclaimed merely because both data legs are temporarily down.

Both children must reach the server port over TCP and UDP. Failover cannot prevent a game's own timeout from disconnecting, and it cannot preserve sockets across a server restart.

## Connection shutdown

DATA_FIN occupies one sequence number at the end of each direction, and the cumulative Data ACK covers it only after every earlier byte has arrived. After an application close, new local I/O is rejected while accepted TX keeps draining; session close is sent after the peer acknowledges the final sequence. A half close does not close the reverse direction.

If FIN and all preceding data have arrived, a later session close or path failure does not discard buffered RX; the application can read to the end. Errors, resets and service shutdown can still abort immediately, and a local application close may discard its own unread RX.

After a full application `Close`, two minutes without cumulative Data ACK progress end the remaining session, so a FIN/ACK is never awaited forever when no path works. This limit does not apply to `CloseWrite`, `CloseRead` or open idle connections.

Application read and write deadlines also cover the first fast-open write. A write timeout returns the accepted prefix; accepted bytes stay queued and the caller may extend the deadline and write the rest. A stream that ends without FIN returns an error rather than a clean EOF.

## Runtime telemetry (status schema 5)

With `status_file` set, the client asks the server in its hello for sender-status frames (schema 4, 372 bytes) on either leg. They report, per logical session, the server's downlink queues, send history, live limits, repairs and reinjections, write stalls and memory pressure. Updates are coalesced and consume no DATA sequence numbers or payload budget. Remote data older than three seconds are marked stale rather than read as zero.

| Location | Main fields |
| --- | --- |
| `node.memory` | `tx_bytes`, `rx_bytes`, `reserved_bytes`, `cached_bytes`, `active_senders`, `active_receivers`, `receive_share_bytes`, `pressure_threshold_bytes` and their peaks |
| `logical.local_sender` / `remote_sender` | `unsent_limit_bytes`, `history_limit_bytes` (largest current value among open sessions), `replay_bytes`, `fallback_*` (repairs), `opportunistic_reinjections`, `tail_reinjections`, `tail_reinjection_bytes`, backpressure count and duration |
| Each leg | `pipeline_limit_bytes` and `outstanding_bytes` (local sender), `remote_pipeline_limit_bytes` and `remote_outstanding_bytes` (peer sender), `feedback_frames_sent/received`, `join_count`, delivery rate and RTT |

Active paths send low-rate PING/PONG probes independently of `status_file`. Reported RTT is the effective application-layer round trip including proxy and transport queueing. Probe timeouts, stalls and reinjections are Multipath-visible events, not raw packet loss. Traffic peaks are the highest one-second averages since start. Remote scheduler rate estimates are not one-second throughput or physical capacity.

Leg joins count successfully attached transports, reconnections included; joins, attempts and remote failures keep the totals of closed connections. Error events carry `last_error_source` (`local_endpoint`, `remote_endpoint`, `transport`, `shutdown` or `unknown`), which is diagnostic only and never changes scheduling or close timing.

At startup each Multipath inbound or outbound logs its budget, emergency margin and cache limit, and it logs once when entering and once when leaving pressure.

## Source map

| Topic | Source |
| --- | --- |
| Client direction policy and validation | [policy.go](../protocol/multipath/policy.go), [option/multipath.go](../option/multipath.go) |
| Hello, authentication and version rejection | [protocol.go](../protocol/multipath/protocol.go), [inbound.go](../protocol/multipath/inbound.go) |
| Session creation and leg management | [recovery_tcp.go](../protocol/multipath/recovery_tcp.go), [outbound.go](../protocol/multipath/outbound.go) |
| Activation | [activation.go](../protocol/multipath/activation.go) |
| Scheduling, feedback and reinjection | [scheduler.go](../protocol/multipath/scheduler.go), [stream/path.go](../protocol/multipath/stream/path.go) |
| Send history and receive storage | [stream/send.go](../protocol/multipath/stream/send.go), [stream/receive.go](../protocol/multipath/stream/receive.go), [core.go](../protocol/multipath/core.go) |
| Application I/O | [logical_conn.go](../protocol/multipath/logical_conn.go) |
| Shared budget and Go memory limit | [memory.go](../protocol/multipath/memory.go), [memory_available_linux.go](../protocol/multipath/memory_available_linux.go), [memory_runtime.go](../protocol/multipath/memory_runtime.go) |
| Failover and session ownership | [recovery_client.go](../protocol/multipath/recovery_client.go), [recovery_policy.go](../protocol/multipath/recovery_policy.go), [recovery_sessions.go](../protocol/multipath/recovery_sessions.go) |
| Shutdown and half close | [lifecycle.go](../protocol/multipath/lifecycle.go) |
| Remote telemetry and client status | [telemetry.go](../protocol/multipath/telemetry.go), [status.go](../protocol/multipath/status.go) |

The [beta5 design](development/multipath-beta5.md) and [validation notes](development/multipath-beta5-validation.md) are historical development documents, not the current configuration reference.
