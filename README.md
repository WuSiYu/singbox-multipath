# singbox-multipath

[中文](#中文) · [English](#english)

## 中文

singbox-multipath 基于 sing-box，在代理层把两条已有路径组合成一条可靠 TCP 字节流，让**单条 TCP 连接**也能利用多条路径的带宽，而不只是把不同连接分配给不同节点。应用和中间代理节点无需支持 MPTCP；客户端和聚合服务端需要运行本项目。

### 机制概览

- **leg0：首选路径。** 适合稳定、低延迟的线路，负责常规建连、控制消息和激活前的数据传输。
- **leg1：扩容路径（booster）。** 达到触发条件后参与传输；调度依据实际交付反馈动态分配数据，不需要手工设置带宽比例。两端按统一字节序号重组数据，应用看到的仍是有序字节流。
- **聚合或切换。** 默认激活后同时使用两条路径；启用 `leg0_traffic_saving` 后改为主要使用 leg1 发送新数据，节省 leg0 流量。控制、补发和故障回退仍可能使用 leg0。
- **可选故障接管。** `failover_enabled` 允许 leg0 失效时由 leg1 承接 TCP 和 UDP，恢复后回到各自首选路径；默认关闭。UDP 不做聚合，也不封装进 MP 的可靠 TCP 字节流。

上传和下载分别配置、分别激活。慢 leg1 不会直接堵住 leg0 的发送线程，但有序交付仍可能等待缺失字节；聚合不保证任何网络下都比单 leg0 更快。本项目借鉴 MPTCP 的连接级机制，并不是内核 MPTCP，也不兼容其线协议。

详细说明：[中文机制文档](docs/multipath.zh-CN.md) · [English mechanism guide](docs/multipath.md)。

OpenWrt 用户可配合 [luci-app-homeproxy-multipath](https://github.com/WuSiYu/luci-app-homeproxy-multipath)，在 LuCI 中配置 Multipath，并查看路径、连接和流量状态。

### 部署与配置示例

两条 child outbound 都必须能够到达同一个 Multipath 服务端。中间可以使用普通 sing-box 节点或其他兼容代理，无需修改中间节点；服务端再通过自己的路由访问目标。

**Multipath 本身不提供认证或加密，监听端口不能直接暴露给不可信网络。** 使用私有链路、带认证的代理和访问控制保护它。双端须使用协议 v12（beta9）；旧协议会被拒绝。

以下是配置片段，需合并到现有 sing-box 配置。客户端仍需配置本地入站，并把需要聚合的流量路由到 `mp-out`。示例用系统 `wg1` 作为 leg0、Hysteria2 作为 leg1；请替换接口、地址、凭据和 TLS 域名，并确保 Hy2 服务端能够访问 `10.66.67.1:39000`。

客户端：上传只走 leg0，下载触发后聚合。将 `download.leg0_traffic_saving` 改为 `true` 可切换为节省 leg0 流量的模式。

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
      "frame_size": "64KB",
      "upload": {
        "aggregation_enabled": false
      },
      "download": {
        "aggregation_enabled": true,
        "leg0_traffic_saving": false,
        "activation_on_queue": true,
        "activation_threshold_mbps": 120,
        "activation_after_bytes": "2MB",
        "activation_after_bytes_min_mbps": 120,
        "activation_window": "1s"
      },
      "memory_limit": 0
    }
  ]
}
```

服务端：只设置监听和本机资源参数，上传/下载策略由客户端传入。

```json
{
  "inbounds": [
    {
      "type": "multipath",
      "tag": "mp-in",
      "listen": "10.66.67.1",
      "listen_port": 39000,
      "tcp_fast_open": true,
      "memory_limit": 0,
      "handshake_timeout": "10s"
    }
  ]
}
```

`tcp_fast_open: true` 允许 Multipath 合并 hello 与首次 DATA 写入；它不会替 child 开启 TCP Fast Open。若希望首批数据进入 TCP SYN，**还要打开 TCP child 的 `tcp_fast_open` 和服务端监听的 TFO**。不支持 TCP TFO 的 child 仍可在其传输建立后使用 MP early-write。MP 的该选项为 false 或省略时，拨号会等待 hello 响应。

需要故障接管时，在客户端的 Multipath outbound 中加入：

```json
{
  "failover_enabled": true,
  "failover_timeout": "5s",
  "failback_delay": "30s"
}
```

这三个字段仅属于客户端，服务端不要填写。开启后，两条 child 均须支持 TCP 和 UDP，且都能到达聚合端口的 TCP/UDP 监听。首次确认首选路径健康时立即选用；已经健康过的路径发生故障后，再次恢复须经过稳定期。UDP 保留 `udp_outbound` 指定的独立首选路径。

### 参数归属

“客户端”指 Multipath outbound，“服务端”指 Multipath inbound；以下方向均从客户端视角命名。

| 客户端策略 | 发送端 | 接收端 |
| --- | --- | --- |
| `upload` | 客户端 | 服务端 |
| `download` | 服务端 | 客户端 |

客户端在握手中提交两份方向策略和共同的 `frame_size`，服务端校验并确认，**不会用自己的方向默认值替换，也不另行协商较小的帧尺寸**。两条 leg 及重连必须保持同一策略；配置非法或服务端会话资源不足时拒绝连接。各主机的 `memory_limit` 独立生效，不被对端覆盖。

自动缓冲上限在拥有该缓冲的主机上解析：例如 `upload.receive_window_bytes: 0` 根据服务端预算确定，`download.receive_window_bytes: 0` 根据客户端预算确定。这里的 0 是自动，不是零容量。方向参数不调整 child 的 TCP/QUIC 缓冲、拥塞控制或 UDP 转发。

### 客户端字段

| 字段 | 作用范围与默认语义 | 可接受格式实例 |
| --- | --- | --- |
| `outbounds` | 客户端本地的两个 child tag，构成双向路径；必须恰好两个，且支持 TCP。 | `["leg0", "leg1"]` |
| `preferred` | 指定 leg0，默认第一个 child；承担初始、控制和首选路径角色。 | `"leg0"` |
| `udp_outbound` | 默认首选 child；关闭故障接管时直接经该 outbound 发 UDP，也可指定第三个 outbound；开启时必须为两个 leg 之一，并同步服务端回包路径。 | `"leg0"`、`"leg1"` |
| `server`、`server_port` | 两个 child 都要访问的聚合服务端地址和端口，必填。 | `"10.66.67.1"`、`39000` |
| `tcp_fast_open` | 客户端逻辑连接 early-write，默认 false；child 和服务端 TCP socket 的 TFO 另行配置。 | `true`、`false` |
| `frame_size` | 客户端选定、服务端确认，双向共用的最大 DATA 载荷；不含帧头。省略/0 为 64 KiB，非零范围 1 KiB–1 MiB；可以发送短帧。 | `65536`、`"64KB"`、`"16 KB"` |
| `upload`、`download` | 客户端提交的每连接方向策略；默认开启聚合、关闭流量节省。 | `{"aggregation_enabled": false}` |
| `status_file` | 客户端本地 JSON 状态文件，默认关闭；每秒更新，包含服务端下行遥测、方向策略和实际缓冲上限。 | `"/var/run/multipath.json"` |
| `failover_enabled` | 客户端 TCP/UDP 接管总开关，默认 false；关闭时不增加共享恢复探测或 UDP 中继。与聚合和流量节省开关独立。 | `true`、`false` |
| `failover_timeout` | 客户端判定路径失效的时限，影响同步给服务端的恢复组租期；默认 5 秒，范围 1 秒–5 分钟。 | `"5s"`、`"10s"` |
| `failback_delay` | 客户端在已发生故障后回到首选路径的连续健康期；默认 30 秒，范围 1 秒–1 小时。首次健康确认不等待；备用也失效时可立即选用健康首选路径。 | `"30s"`、`"1m"` |

### upload / download 字段

这些字段**只在客户端配置**，随后在对应方向的发送端或接收端执行。缓冲上限按每个逻辑连接、每个方向计算。

| 字段 | 作用位置与默认语义 | 可接受格式实例 |
| --- | --- | --- |
| `aggregation_enabled` | 发送端总开关，默认 true；false 时新数据保持 leg0，独立的故障接管仍可使用 leg1。 | `true`、`false` |
| `leg0_traffic_saving` | 发送端选路，默认 false；true 时激活后改为 leg1 独占新数据，而非双路径聚合；本字段不独立触发激活。 | `true`、`false` |
| `activation_on_queue` | 发送端**条件 1**，默认 true：leg0 在途量与本地待发送量之和连续一个窗口达到 `queue_frames * frame_size` 的 80%。 | `true`、`false` |
| `activation_threshold_mbps` | 发送端**条件 2**：窗口内平均接收应用数据的速率达到阈值。省略时默认 150 Mbps；若已启用累计字节触发，则省略时为 0。显式 0 关闭。 | `0`、`120` |
| `activation_after_bytes` | 发送端**条件 3**：累计接收应用字节数达到阈值，并满足下一项的最低速率；省略/0 关闭。 | `0`、`2097152`、`"2MB"` |
| `activation_after_bytes_min_mbps` | 仅作为条件 3 内部的“且”条件，默认 0；非零时还要求完整采样窗口内的平均速率达标，不限制条件 1、2。 | `0`、`5`、`120` |
| `activation_window` | 发送端的排队持续期和速率采样窗口，省略/0 为 1 秒。 | `"1s"`、`"500ms"` |
| `queue_frames` | 发送端待发送容量，以 `frame_size` 为单位；省略/0 为 256，非零范围 8–4096，乘积不超过 64 MiB。这部分容量包含在 `send_buffer_bytes` 内，不是接收帧数或在途上限。 | `64`、`256` |
| `send_buffer_bytes` | 发送端两条路径共用的连接发送历史，包含待发送字节；收到累计 Data ACK 后才可释放。省略/0 在发送端自动解析，非零范围为 `frame_size`–512 MiB。 | `0`、`67108864`、`"64MB"` |
| `receive_window_bytes` | 接收端连接字节窗口，约束该方向发送量，包含按序但尚未被应用读取的数据。省略/0 在接收端自动解析，非零范围为 `frame_size`–512 MiB；没有帧数限制。 | `0`、`134217728`、`"128MB"` |
| `path_stall_timeout_min` | 发送端对任一 leg 的无进展检测下限。省略/0 使用自适应值；非零范围 100 ms–5 分钟。不是固定重传间隔或断连倒计时。 | `"0s"`、`"500ms"`、`"2s"` |

激活逻辑是 **总开关开启，且（条件 1 或条件 2 或条件 3）**，按每条连接、每个方向独立判断。三个条件全部关闭就不会激活；数值触发器为 0 不代表立即激活。激活后不会因速率下降而自动退回未激活状态。流量节省模式中的故障回退和恢复不改变这一点。

### 本机资源与服务端监听

| 字段 | 作用范围与默认语义 | 可接受格式实例 |
| --- | --- | --- |
| `memory_limit` | 本机每个 MP inbound/outbound 的共享预算，涵盖其 TCP 和恢复 UDP 会话；不协商、不被对端覆盖。省略/0 为 `min(512 MiB, 可用内存 × 0.5)`。Linux 同时考虑 MemAvailable 和可见 cgroup 剩余额度；不是 RSS 或 child 缓冲上限。 | `0`、`268435456`、`"256MB"` |
| `handshake_timeout` | 各主机独立的 leg 握手时限；默认 10 秒，正值范围 1–60 秒。 | `"10s"`、`"5s"` |
| `listen`、`listen_port` | 仅服务端，标准 sing-box TCP/UDP 监听字段；开启故障接管时须让两个 child 均能访问 TCP 和 UDP。 | `"10.66.67.1"`、`39000` |
| 服务端 `tcp_fast_open` | 服务端 TCP socket 选项，不属于客户端下发的方向策略。 | `true`、`false` |

容量字符串使用整数和二进制单位：`"2MB"`、`"2 MB"` 都是 2,097,152 字节；支持不区分大小写的 `B`、`K/KB`、`M/MB` 至 `E/EB`，**不接受 `KiB/MiB` 或小数**。速率单位 Mbps 则是每秒 1,000,000 bit。

自动缓冲值是上限，不会预分配对应大小；所有连接仍共享本机预算。1 GiB 主机可从 `"memory_limit": "256MB"` 起步，为 child 协议、其他进程和内核留出空间。自动 Go 软限制及其边界见[内存机制](docs/multipath.zh-CN.md#内存与反压)。

### 旧配置迁移

旧版平铺的 `aggregation_enabled`、全部 `activation_*` 和 `queue_frames` 只告警并忽略，需移入客户端的 `upload` / `download`；服务端不再配置方向策略。以下旧字段同样只告警并忽略，不会自动迁移或作为新字段默认值：

| 旧字段 | 当前写法 |
| --- | --- |
| `chunk_size` | 客户端顶层 `frame_size` |
| `leg1_replay_bytes` | 方向内的 `send_buffer_bytes`，两条路径共用 |
| `max_reorder_bytes` | 方向内的 `receive_window_bytes`，由该方向接收端执行 |
| `leg1_replay_timeout` | 方向内的 `path_stall_timeout_min`，可作用于任一 leg |
| `bandwidth_mbps` | 删除；调度自动测量，没有带宽比例或限速替代项 |
| `max_reorder_frames` | 删除；不提供 `receive_window_frames` |

旧帧尺寸、重排、replay 和带宽字段即使放进方向对象也无效。未知字段或非法的新字段值仍会报错；服务端的 `failover_enabled` 也应删除。原生 sing-box 功能见[上游文档](https://sing-box.sagernet.org/)。

## English

singbox-multipath extends sing-box with application-layer aggregation over two existing paths, allowing **a single TCP connection** to use their combined capacity instead of merely distributing separate connections between nodes. Applications and intermediate proxies do not need MPTCP support; the client and aggregation server run this project.

### Overview

- **leg0 is the preferred path:** normally a stable, low-latency link for session setup, control traffic and data before activation.
- **leg1 is the capacity booster:** it carries data after activation. Scheduling follows measured end-to-end delivery rather than configured bandwidth weights; both endpoints reconstruct one ordered byte stream.
- **Aggregate or switch:** the default is aggregation after activation. `leg0_traffic_saving` instead assigns new data to leg1 when eligible, saving leg0 traffic. Control, reinjection and fallback can still use leg0.
- **Optional failover:** `failover_enabled` lets leg1 take over TCP and UDP during a leg0 outage, then restores each protocol's preferred path. It is off by default. UDP is not aggregated or carried inside the MP reliable TCP stream.

Upload and download activate independently. A blocked leg1 writer does not directly block the leg0 writer, but ordered delivery can still wait for missing bytes; aggregation is not guaranteed to outperform leg0 alone on every network. This is inspired by MPTCP's connection-level mechanisms, not kernel MPTCP or its wire protocol.

Details: [English mechanism guide](docs/multipath.md) · [中文机制文档](docs/multipath.zh-CN.md).

On OpenWrt, [luci-app-homeproxy-multipath](https://github.com/WuSiYu/luci-app-homeproxy-multipath) provides LuCI configuration and a dashboard for Multipath paths, connections and traffic.

### Deployment and examples

Both child outbounds must reach the same Multipath server, which uses its own routing to reach application destinations. Intermediate nodes may be ordinary, unmodified proxy nodes.

**Multipath provides no authentication or encryption of its own. Do not expose its listener to untrusted networks.** Protect it with private paths, authenticated proxies and access controls. Both endpoints must use protocol v12 (beta9); older versions are rejected.

These are configuration fragments to merge into an existing sing-box configuration. Add a client inbound and route the desired traffic to `mp-out`. Replace the interface, addresses, credentials and TLS name; the Hy2 server must be able to reach `10.66.67.1:39000`.

Client: a system `wg1` interface is leg0 and Hysteria2 is leg1. Upload stays on leg0; download aggregates after activation. Set `download.leg0_traffic_saving` to `true` to switch to leg1 instead.

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
      "frame_size": "64KB",
      "upload": {
        "aggregation_enabled": false
      },
      "download": {
        "aggregation_enabled": true,
        "leg0_traffic_saving": false,
        "activation_on_queue": true,
        "activation_threshold_mbps": 120,
        "activation_after_bytes": "2MB",
        "activation_after_bytes_min_mbps": 120,
        "activation_window": "1s"
      },
      "memory_limit": 0
    }
  ]
}
```

Server: configure only the listener and host-local resource limits; directional policies arrive from the client.

```json
{
  "inbounds": [
    {
      "type": "multipath",
      "tag": "mp-in",
      "listen": "10.66.67.1",
      "listen_port": 39000,
      "tcp_fast_open": true,
      "memory_limit": 0,
      "handshake_timeout": "10s"
    }
  ]
}
```

Multipath `tcp_fast_open` combines hello with the first DATA write; it does not enable TFO inside a child. For payload in a TCP SYN, **also enable the TCP child's `tcp_fast_open` and the server listener's TFO**. A child without TCP TFO can still carry MP early-write after establishing its transport. With MP TFO false or omitted, Dial waits for the hello response.

For failover, add these fields to the client Multipath outbound:

```json
{
  "failover_enabled": true,
  "failover_timeout": "5s",
  "failback_delay": "30s"
}
```

Do not put these fields on the server. Both children must support TCP and UDP and reach both transports on the aggregation port. First healthy discovery selects the preferred path without a return delay; after a previously healthy path fails, later recovery uses the stability hold. UDP retains the independent preference set by `udp_outbound`.

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
| `failover_timeout` | Client path failure detection; sent to recovery group. | 5 seconds, range 1 second–5 minutes; requires failover enabled. | `"5s"`, `"10s"` |
| `failback_delay` | Client preferred-path stability hold after a previously healthy path fails; synchronized with server. | 30 seconds, range 1 second–1 hour; requires failover enabled. First healthy discovery at startup has no hold. It restores normal policy, not forced leg 0 DATA in an activated saving direction. | `"30s"`, `"1m"` |

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
| `handshake_timeout` | Local leg-handshake deadline, independent on each host. | 10 seconds; omitted/zero default, positive range 1–60 seconds. | `"10s"`, `"5s"` |
| `listen`, `listen_port` | Server-local TCP and UDP listener. | Standard sing-box listen fields; reachable through both children when failover is used. | `"10.66.67.1"`, `39000` |
| Server `tcp_fast_open` | Server TCP socket option, not a directional policy. | Standard listener option. | `true`, `false` |

Memory strings use binary units: `"2MB"` and `"2 MB"` both mean 2,097,152 bytes.
Suffixes are case-insensitive `B`, `K`/`KB`, `M`/`MB`, through `E`/`EB`.
The memory parser does not accept `KiB`/`MiB` suffixes or fractional quantities.
Rates in Mbps use 1,000,000 bits per second.
Automatic buffer limits are ceilings, not preallocations. A smaller shared budget
can apply backpressure before any one connection reaches its ceiling. On a 1 GiB
host, `"memory_limit": "256MB"` is a conservative starting point. The automatic Go
soft limit and its boundaries are described in [Memory and backpressure](docs/multipath.md#memory-and-backpressure).

### Legacy configuration

The parser recognizes the following **old flat fields only to warn and ignore them**:
`aggregation_enabled`, all five `activation_*` fields, `queue_frames`,
`chunk_size`, `max_reorder_frames`, `max_reorder_bytes`,
`leg1_replay_bytes`, `leg1_replay_timeout`, and `bandwidth_mbps`.
The old names for frame/reorder/replay/weights are also ignored with a warning if
placed inside a direction object. They are not migrated, used as defaults, or
allowed to override new fields. Even an invalid legacy value is ignored.

Reconfigure both directions on the client and remove directional tuning from the server.

| Old field | Current configuration |
| --- | --- |
| `chunk_size` | Client top-level `frame_size` |
| `leg1_replay_bytes` | Directional `send_buffer_bytes`, shared by both paths |
| `max_reorder_bytes` | Directional `receive_window_bytes`, enforced by that direction's receiver |
| `leg1_replay_timeout` | Directional `path_stall_timeout_min`, applicable to either leg |
| `bandwidth_mbps` | Remove; scheduling is automatic, with no replacement weight or rate cap |
| `max_reorder_frames` | Remove; there is no `receive_window_frames` option |

Unknown fields and invalid new-field values are errors. Remove server-side
`failover_enabled` as well. Upgrade both endpoints together when changing wire
versions; old protocols are rejected immediately.

Upstream features: [sing-box documentation](https://sing-box.sagernet.org/).

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
