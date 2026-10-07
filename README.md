# singbox-multipath

[中文](#中文) · [English](#english)

## 中文

singbox-multipath 基于 sing-box，在代理层把两条已有路径组合成一条可靠 TCP 字节流，让**单条 TCP 连接**也能利用两条路径的带宽，而不只是把不同连接分配给不同节点。应用和中间代理节点无需支持 MPTCP；客户端和聚合服务端需要运行本项目。

### 机制概览

- **leg0：首选路径。** 常规建连和激活前的应用数据走它，适合稳定、低延迟的线路。
- **leg1：扩容路径。** leg0 成为瓶颈后参与传输；调度按实际交付反馈选择预计最早送达的路径（ECF），不需要手工设置带宽比例。两端按统一字节序号重组数据，应用看到的仍是有序字节流。
- **两条 leg 对等承载控制。** 确认、窗口、FIN 和关闭可以走任一 leg；任一 leg 断开后自动重连并重新挂入原会话，**单条 leg 丢失不会中断连接**。
- **聚合或切换。** 默认激活后同时使用两条路径；启用 `leg0_traffic_saving` 后改为主要使用 leg1 发送新数据，节省 leg0 流量。
- **可选故障接管。** `failover_enabled` 增加共享健康检查、UDP 中继与 UDP 路径切换；默认关闭。UDP 不做聚合，也不封装进 MP 的可靠字节流。

上传和下载分别配置、分别激活。有序交付仍可能等待缺失字节；两条路径共享物理瓶颈时也无法获得带宽之和。本项目借鉴 MPTCP 的连接级机制，并不是内核 MPTCP，也不兼容其线协议。

详细说明：[中文机制文档](docs/multipath.zh-CN.md) · [English mechanism guide](docs/multipath.md)。

OpenWrt 用户可配合 [luci-app-homeproxy-multipath](https://github.com/WuSiYu/luci-app-homeproxy-multipath)，在 LuCI 中配置 Multipath，并查看路径、连接和流量状态。

### 部署与配置示例

两条 child outbound 都必须能够到达同一个 Multipath 服务端。中间可以使用普通 sing-box 节点或其他兼容代理，无需修改中间节点；服务端再通过自己的路由访问目标。

**Multipath 不加密数据，未加保护的监听端口会被任何能访问它的人用来转发流量。** 请在两端配置相同的 `psk`（每个握手都经 HMAC-SHA256 认证并防重放），并在服务端用 `allowed_ips` 或防火墙限制来源；数据加密由 child 协议或私有链路负责。双端须使用协议 v13（beta10）；版本不一致时服务端会明确拒绝并说明原因。

以下是配置片段，需合并到现有 sing-box 配置。客户端仍需配置本地入站，并把需要聚合的流量路由到 `mp-out`。示例中两条 leg 分别绑定两个 WAN 接口，直连到服务端；请替换接口、地址和密钥。两条 leg 走同一出口时无法聚合两条线路。

客户端：默认值即推荐值，通常只需填写必需字段。

```json
{
  "outbounds": [
    { "type": "direct", "tag": "wan1-leg", "bind_interface": "wan" },
    { "type": "direct", "tag": "wan2-leg", "bind_interface": "wan2" },
    {
      "type": "multipath",
      "tag": "mp-out",
      "outbounds": ["wan1-leg", "wan2-leg"],
      "server": "203.0.113.10",
      "server_port": 39000,
      "psk": "change-me-to-a-long-random-string"
    }
  ]
}
```

服务端：只设置监听、认证和本机资源参数，上传/下载策略由客户端传入。

```json
{
  "inbounds": [
    {
      "type": "multipath",
      "tag": "mp-in",
      "listen": "::",
      "listen_port": 39000,
      "psk": "change-me-to-a-long-random-string",
      "allowed_ips": ["198.51.100.0/24", "2001:db8::/32"]
    }
  ]
}
```

child 可以是任何支持 TCP 的 outbound，例如加密的 Shadowsocks、Hysteria2 或 WireGuard 接口；它们决定数据在公网上的加密方式。上传和下载可以分别设置，例如只聚合下载：

```json
{
  "upload": { "aggregation_enabled": false },
  "download": { "aggregation_enabled": true }
}
```

客户端 `tcp_fast_open`（默认开启）把 hello 与首次 DATA 合并写入，节省一个往返；它不会替 child 开启 TCP Fast Open。若希望首批数据进入 TCP SYN，还要打开 TCP child 的 `tcp_fast_open` 和服务端监听的 TFO。

需要故障接管时，在客户端的 Multipath outbound 中加入：

```json
{
  "failover_enabled": true,
  "failover_timeout": "5s",
  "failback_delay": "30s"
}
```

这三个字段仅属于客户端。开启后，两条 child 均须支持 TCP 和 UDP，且都能到达聚合端口的 TCP/UDP 监听。

### 参数归属

“客户端”指 Multipath outbound，“服务端”指 Multipath inbound；以下方向均从客户端视角命名。

| 客户端策略 | 发送端 | 接收端 |
| --- | --- | --- |
| `upload` | 客户端 | 服务端 |
| `download` | 服务端 | 客户端 |

客户端在握手中提交两份方向策略和共同的 `frame_size`，服务端校验并确认，不会用自己的默认值替换。两条 leg 及重连必须保持同一策略；配置非法或服务端会话资源不足时拒绝连接，并返回原因。各主机的 `memory_limit` 独立生效，不被对端覆盖。缓冲上限在拥有该缓冲的主机上解析，例如 `upload.receive_window_bytes` 由服务端执行。方向参数不调整 child 的 TCP/QUIC 缓冲、拥塞控制或 UDP 转发。

### 客户端字段

| 字段 | 作用范围与默认语义 | 可接受格式实例 |
| --- | --- | --- |
| `outbounds` | 两个 child tag，必须恰好两个且支持 TCP。 | `["leg0", "leg1"]` |
| `preferred` | 指定 leg0（首选路径），默认第一个 child。 | `"leg0"` |
| `udp_outbound` | UDP 出口，默认首选 child；关闭故障接管时可指定第三个 outbound，开启时必须为两个 leg 之一。 | `"leg0"`、`"leg1"` |
| `server`、`server_port` | 两个 child 都要访问的聚合服务端，必填。 | `"203.0.113.10"`、`39000` |
| `psk` | 预共享密钥，须与服务端一致；为空时不认证。 | `"a-long-random-string"` |
| `tcp_fast_open` | MP 层 early-write：hello 与首次 DATA 合并，默认 true。child 和服务端 socket 的 TFO 另行配置。 | `true`、`false` |
| `frame_size` | 双向共用的最大 DATA 载荷，不含帧头。默认 64 KiB，非零范围 1 KiB–1 MiB；可以发送短帧。 | `65536`、`"64KB"` |
| `upload`、`download` | 每连接方向策略，字段见下表。 | `{"aggregation_enabled": false}` |
| `status_file` | 本地 JSON 状态文件（schema 5），默认关闭；每秒更新，包含服务端下行遥测。 | `"/var/run/multipath.json"` |
| `memory_limit`、`handshake_timeout` | 本机资源参数，见下文。 | |
| `failover_enabled` | 共享健康检查、UDP 中继与路径切换，默认 false。会话在任一 leg 上存活不依赖此开关。 | `true`、`false` |
| `failover_timeout` | 判定路径失效的时限；默认 5 秒，范围 1 秒–5 分钟。 | `"5s"` |
| `failback_delay` | 首选路径反复故障后回切所需的最长稳定期；首次 3 秒，反复故障翻倍，至多该值。默认 30 秒，范围 1 秒–1 小时。 | `"30s"`、`"1m"` |

### upload / download 字段

这些字段**只在客户端配置**，随后在对应方向的发送端或接收端执行。上限按每个逻辑连接、每个方向计算。**默认值即推荐值**，下列上限字段通常保持省略。

| 字段 | 作用位置与默认语义 | 可接受格式实例 |
| --- | --- | --- |
| `aggregation_enabled` | 发送端总开关，默认 true；false 时新数据保持在 leg0（leg0 失效时仍转到 leg1）。 | `true`、`false` |
| `leg0_traffic_saving` | 发送端选路，默认 false；true 时激活后改为 leg1 独占新数据。 | `true`、`false` |
| `activation_on_queue` | **条件 1**，默认 true：本地未发送数据持续一个窗口不低于未发送上限的一半，且 leg0 交付速率已不再增长（leg0 已是瓶颈）。 | `true`、`false` |
| `activation_threshold_mbps` | **条件 2**：窗口内平均应用数据速率达到阈值。默认 0（关闭）。 | `0`、`150` |
| `activation_after_bytes` | **条件 3**：累计应用字节达到阈值；省略/0 关闭。 | `"2MB"` |
| `activation_after_bytes_min_mbps` | 条件 3 内部的“且”条件，默认 0；非零时还要求完整窗口内的平均速率达标。 | `0`、`120` |
| `activation_window` | 排队持续期和速率采样窗口，默认 200 ms，范围 20 ms–10 秒。 | `"200ms"`、`"1s"` |
| `queue_frames` | 本地未发送数据的**上限**（以 `frame_size` 计），默认 256。实际值约为路径交付速率 × 10 ms，至少 1 MiB。范围 8–4096，乘积不超过 64 MiB。 | `256` |
| `send_buffer_bytes` | 连接发送历史（两条路径共用、含未发送数据）的**上限**；省略时为 512 MiB。实际值约为两倍带宽时延积，并受本机按需求公平分配（max-min）的份额约束。 | `0`、`"64MB"` |
| `receive_window_bytes` | 接收窗口的**上限**；省略时为 512 MiB。实际值为本会话在接收端按需求公平分配（max-min）的份额。 | `0`、`"128MB"` |
| `path_stall_timeout_min` | 无进展检测的下限；省略时自适应，非零范围 100 ms–5 分钟。不是固定重传间隔。 | `"500ms"` |

激活逻辑是 **总开关开启，且（条件 1 或条件 2 或条件 3）**，按每条连接、每个方向独立判断。三个条件全部关闭就不会激活。激活后不会因速率下降而退回。需要 beta10 之前“1 秒内 150 Mbps 才激活”的行为，可显式写 `"activation_threshold_mbps": 150, "activation_window": "1s"`。

### 本机资源与服务端字段

| 字段 | 作用范围与默认语义 | 可接受格式实例 |
| --- | --- | --- |
| `memory_limit` | 本机每个 MP inbound/outbound 的共享预算；不协商、不被对端覆盖。省略/0 为 `min(512 MiB, 可用内存 × 0.5)`，Linux 同时考虑 cgroup 剩余额度。不是 RSS 或 child 缓冲上限。 | `0`、`"256MB"` |
| `handshake_timeout` | 各主机独立的 leg 握手时限；默认 10 秒，范围 1–60 秒。 | `"10s"` |
| `listen`、`listen_port` | 仅服务端，标准 sing-box TCP/UDP 监听字段。 | `"::"`、`39000` |
| 服务端 `psk` | 与客户端一致的预共享密钥；设置后拒绝未认证或签名错误的 hello。 | `"a-long-random-string"` |
| 服务端 `allowed_ips` | 只接受这些来源前缀的 child 连接和故障接管 UDP 中继报文；为空时不限制。 | `["198.51.100.0/24"]` |
| 服务端 `tcp_fast_open` | 服务端 TCP socket 选项。 | `true`、`false` |

服务端既没有 `psk`、`allowed_ips`，又监听在非私有地址时，启动日志会警告。

容量字符串使用整数和二进制单位：`"2MB"`、`"2 MB"` 都是 2,097,152 字节；支持不区分大小写的 `B`、`K/KB`、`M/MB` 至 `E/EB`，**不接受 `KiB/MiB` 或小数**。速率单位 Mbps 是每秒 1,000,000 bit。1 GiB 主机可从 `"memory_limit": "256MB"` 起步。内存分区和自动 Go 软限制见[内存机制](docs/multipath.zh-CN.md#内存与反压)。

### 从 beta9 升级

- **两端须同时升级**：协议升到 v13，旧版本会被明确拒绝。状态文件升到 schema 5（LuCI 须同步升级）。
- **默认值变化**：`activation_window` 1 秒 → 200 ms；`activation_threshold_mbps` 默认 150 → 0（不再按速率激活，改为 leg0 成为瓶颈时激活）；客户端 `tcp_fast_open` 默认 false → true。
- **语义变化**：`queue_frames`、`send_buffer_bytes`、`receive_window_bytes` 由容量变为上限，实际值自动跟随路径与内存；旧配置里为“调大缓冲”写的数值可以删除。内存压力不再关闭 leg1 或冻结接收窗口。
- **会话**：leg0 丢失不再关闭连接；未开启故障接管时，两条 leg 都缺失超过 `max(30 秒, 3 × handshake_timeout)` 才结束会话。
- **新字段**：两端 `psk`，服务端 `allowed_ips`。

### 旧配置迁移

旧版平铺的 `aggregation_enabled`、全部 `activation_*` 和 `queue_frames` 只告警并忽略，需移入客户端的 `upload` / `download`；服务端不再配置方向策略。以下旧字段同样只告警并忽略：

| 旧字段 | 当前写法 |
| --- | --- |
| `chunk_size` | 客户端顶层 `frame_size` |
| `leg1_replay_bytes` | 方向内的 `send_buffer_bytes`，两条路径共用 |
| `max_reorder_bytes` | 方向内的 `receive_window_bytes`，由该方向接收端执行 |
| `leg1_replay_timeout` | 方向内的 `path_stall_timeout_min`，可作用于任一 leg |
| `bandwidth_mbps` | 删除；调度自动测量，没有带宽比例或限速替代项 |
| `max_reorder_frames` | 删除；不提供 `receive_window_frames` |

未知字段或非法的新字段值仍会报错；服务端的 `failover_enabled` 也应删除。原生 sing-box 功能见[上游文档](https://sing-box.sagernet.org/)。

## English

singbox-multipath extends sing-box with application-layer aggregation over two existing paths, allowing **a single TCP connection** to use their combined capacity instead of merely distributing separate connections between nodes. Applications and intermediate proxies do not need MPTCP support; the client and aggregation server run this project.

### Overview

- **Leg 0 is the preferred path:** session setup and application data before activation use it; it is normally the stable, low-latency link.
- **Leg 1 adds capacity:** it joins once leg 0 is the bottleneck. Scheduling sends each segment on the path expected to deliver it first (ECF), based on measured end-to-end delivery rather than configured weights; both endpoints reconstruct one ordered byte stream.
- **Both legs carry control:** acknowledgements, windows, FIN and close travel on either leg, and a lost leg is redialed and rejoins its session, so **losing one leg does not break the connection**.
- **Aggregate or switch:** the default is aggregation after activation. `leg0_traffic_saving` instead sends new data on leg 1 to save leg 0 traffic.
- **Optional failover:** `failover_enabled` adds shared health checks, a UDP relay and UDP path switching. It is off by default. UDP is not aggregated or carried inside the reliable stream.

Upload and download are configured and activated independently. Ordered delivery can still wait for missing bytes, and two paths sharing a physical bottleneck cannot add up their capacity. This is inspired by MPTCP's connection-level mechanisms, not kernel MPTCP or its wire protocol.

Details: [English mechanism guide](docs/multipath.md) · [中文机制文档](docs/multipath.zh-CN.md).

On OpenWrt, [luci-app-homeproxy-multipath](https://github.com/WuSiYu/luci-app-homeproxy-multipath) provides LuCI configuration and a dashboard for Multipath paths, connections and traffic.

### Deployment and examples

Both child outbounds must reach the same Multipath server, which uses its own routing to reach application destinations. Intermediate nodes may be ordinary, unmodified proxy nodes.

**Multipath does not encrypt data, and an unprotected listener lets anyone who can reach it relay traffic.** Configure the same `psk` on both endpoints (every handshake is then authenticated with HMAC-SHA256 and replay-checked) and restrict sources on the server with `allowed_ips` or a firewall; encryption is the job of the child protocols or private links. Both endpoints must run protocol v13 (beta10); a version mismatch is rejected with an explicit reason.

These are configuration fragments to merge into an existing sing-box configuration. Add a client inbound and route the desired traffic to `mp-out`. The example binds the two legs to two WAN interfaces and connects them directly to the server; replace interfaces, addresses and the key. Two legs leaving through the same uplink cannot aggregate two lines.

Client: the defaults are the recommended values, so usually only the required fields are needed.

```json
{
  "outbounds": [
    { "type": "direct", "tag": "wan1-leg", "bind_interface": "wan" },
    { "type": "direct", "tag": "wan2-leg", "bind_interface": "wan2" },
    {
      "type": "multipath",
      "tag": "mp-out",
      "outbounds": ["wan1-leg", "wan2-leg"],
      "server": "203.0.113.10",
      "server_port": 39000,
      "psk": "change-me-to-a-long-random-string"
    }
  ]
}
```

Server: configure only the listener, authentication and host-local resources; directional policies arrive from the client.

```json
{
  "inbounds": [
    {
      "type": "multipath",
      "tag": "mp-in",
      "listen": "::",
      "listen_port": 39000,
      "psk": "change-me-to-a-long-random-string",
      "allowed_ips": ["198.51.100.0/24", "2001:db8::/32"]
    }
  ]
}
```

A child can be any TCP-capable outbound, such as encrypted Shadowsocks, Hysteria2 or a WireGuard interface; children decide how data are protected on the public network. Upload and download can differ, for example aggregating downloads only:

```json
{
  "upload": { "aggregation_enabled": false },
  "download": { "aggregation_enabled": true }
}
```

The client's `tcp_fast_open` (on by default) writes the hello together with the first DATA and saves a round trip; it does not enable TCP Fast Open inside a child. For payload in a TCP SYN, also enable the TCP child's `tcp_fast_open` and the server listener's TFO.

For failover, add these fields to the client Multipath outbound:

```json
{
  "failover_enabled": true,
  "failover_timeout": "5s",
  "failback_delay": "30s"
}
```

They are client-only. Both children must then support TCP and UDP and reach both transports on the aggregation port.

### Parameter ownership

The client is the Multipath outbound; the server is the Multipath inbound. Directions are named from the client's point of view.

| Client policy | Sender | Receiver |
| --- | --- | --- |
| `upload` | Client | Server |
| `download` | Server | Client |

The client sends both policies and a common `frame_size` in its hello; the server validates and confirms them without substituting its own defaults. Both legs and every reattachment must match, and invalid policies or insufficient server memory reject the connection with a reason. Each host's `memory_limit` stays independent. A buffer ceiling is enforced by the host that owns that buffer, for example `upload.receive_window_bytes` by the server. Policies do not tune child TCP/QUIC buffers, congestion control or UDP forwarding.

### Client fields

| Field | Scope and default | Examples |
| --- | --- | --- |
| `outbounds` | Exactly two TCP-capable child tags. | `["leg0", "leg1"]` |
| `preferred` | Leg 0 (the preferred path); defaults to the first child. | `"leg0"` |
| `udp_outbound` | UDP outbound, default the preferred child. Without failover it may name a third outbound; with failover it must be one of the two legs. | `"leg0"`, `"leg1"` |
| `server`, `server_port` | Aggregation server reached through both children; required. | `"203.0.113.10"`, `39000` |
| `psk` | Pre-shared key, must match the server; empty disables authentication. | `"a-long-random-string"` |
| `tcp_fast_open` | Multipath early write: hello merged with the first DATA, default true. Child and server socket TFO are separate. | `true`, `false` |
| `frame_size` | Largest DATA payload in both directions, excluding headers. Default 64 KiB, range 1 KiB–1 MiB; shorter frames are allowed. | `65536`, `"64KB"` |
| `upload`, `download` | Per-connection direction policies; fields below. | `{"aggregation_enabled": false}` |
| `status_file` | Local JSON status (schema 5), off by default; updated every second, including server downlink telemetry. | `"/var/run/multipath.json"` |
| `memory_limit`, `handshake_timeout` | Host-local resources, see below. | |
| `failover_enabled` | Shared health checks, UDP relay and path switching, default false. Sessions survive on either leg without it. | `true`, `false` |
| `failover_timeout` | Path failure detection, default 5 s, range 1 s–5 min. | `"5s"` |
| `failback_delay` | Longest stability hold before returning to a preferred path that failed repeatedly: 3 s at first, doubling up to this value. Default 30 s, range 1 s–1 h. | `"30s"`, `"1m"` |

### Fields inside upload and download

These fields are set **only on the client** and applied by the direction's sender or receiver. Limits are per logical connection and direction. **The defaults are the recommended values**; leave the ceilings unset unless you need a hard cap.

| Field | Application and default | Examples |
| --- | --- | --- |
| `aggregation_enabled` | Sender master switch, default true; false keeps new data on leg 0 (still moving to leg 1 if leg 0 fails). | `true`, `false` |
| `leg0_traffic_saving` | Sender path selection, default false; true sends new data only on leg 1 after activation. | `true`, `false` |
| `activation_on_queue` | **Condition 1**, default true: unsent data stay at or above half the unsent limit for a window and leg 0's delivery rate has stopped growing (leg 0 is the bottleneck). | `true`, `false` |
| `activation_threshold_mbps` | **Condition 2**: average application data rate over the window reaches the threshold. Default 0 (off). | `0`, `150` |
| `activation_after_bytes` | **Condition 3**: cumulative application bytes reach the threshold; 0 or omitted disables. | `"2MB"` |
| `activation_after_bytes_min_mbps` | Extra AND gate within condition 3, default 0; otherwise also requires that average rate over a complete window. | `0`, `120` |
| `activation_window` | Queue duration and rate sampling window, default 200 ms, range 20 ms–10 s. | `"200ms"`, `"1s"` |
| `queue_frames` | **Ceiling** of unsent data in `frame_size` units, default 256. The working value is about 10 ms of delivery rate, at least 1 MiB. Range 8–4096; the product may not exceed 64 MiB. | `256` |
| `send_buffer_bytes` | **Ceiling** of the send history shared by both paths, unsent data included; 512 MiB when omitted. The working value is about twice the bandwidth-delay product within the host's max-min fair share. | `0`, `"64MB"` |
| `receive_window_bytes` | **Ceiling** of the receive window; 512 MiB when omitted. The working value is the session's max-min fair share on the receiver. | `0`, `"128MB"` |
| `path_stall_timeout_min` | Floor of no-progress detection; adaptive when omitted, otherwise 100 ms–5 min. Not a fixed retransmission interval. | `"500ms"` |

Activation is **master switch AND (condition 1 OR condition 2 OR condition 3)**, per connection and direction. Disabling all three prevents activation, and an activated direction stays activated. For the pre-beta10 "150 Mbps over one second" behavior, set `"activation_threshold_mbps": 150, "activation_window": "1s"` explicitly.

### Host-local and server fields

| Field | Scope and default | Examples |
| --- | --- | --- |
| `memory_limit` | Shared budget of each local Multipath inbound/outbound; never negotiated. Omitted/0 means `min(512 MiB, available memory × 0.5)`, with cgroup headroom on Linux. Not an RSS or child-buffer cap. | `0`, `"256MB"` |
| `handshake_timeout` | Leg handshake deadline on each host, default 10 s, range 1–60 s. | `"10s"` |
| `listen`, `listen_port` | Server only; standard sing-box TCP/UDP listen fields. | `"::"`, `39000` |
| Server `psk` | Pre-shared key matching the client; unauthenticated or wrongly signed hellos are rejected. | `"a-long-random-string"` |
| Server `allowed_ips` | Accept child connections and failover UDP relay datagrams only from these source prefixes; empty means unrestricted. | `["198.51.100.0/24"]` |
| Server `tcp_fast_open` | Server TCP socket option. | `true`, `false` |

A server without `psk` or `allowed_ips` listening on a non-private address logs a warning.

Memory strings use integers with binary units: `"2MB"` and `"2 MB"` both mean 2,097,152 bytes. Case-insensitive `B`, `K`/`KB`, `M`/`MB` through `E`/`EB` are accepted; `KiB`/`MiB` and fractions are not. Mbps means 1,000,000 bits per second. On a 1 GiB host, start with `"memory_limit": "256MB"`. The memory regions and the automatic Go soft limit are described in [Memory and backpressure](docs/multipath.md#memory-and-backpressure).

### Upgrading from beta9

- **Upgrade both endpoints together**: the protocol is now v13 and older versions are rejected explicitly. The status file is schema 5 (upgrade LuCI as well).
- **Changed defaults**: `activation_window` 1 s → 200 ms; `activation_threshold_mbps` 150 → 0 (activation follows leg 0 becoming the bottleneck instead of a rate); client `tcp_fast_open` false → true.
- **Changed meaning**: `queue_frames`, `send_buffer_bytes` and `receive_window_bytes` are ceilings instead of capacities, and the working values follow paths and memory automatically; values set only to enlarge buffers can be removed. Memory pressure no longer disables leg 1 or freezes receive windows.
- **Sessions**: losing leg 0 no longer closes a connection; without failover a session ends after both legs have been absent for `max(30 s, 3 × handshake_timeout)`.
- **New fields**: `psk` on both endpoints, `allowed_ips` on the server.

### Legacy configuration

The parser recognizes the following **old flat fields only to warn and ignore them**: `aggregation_enabled`, all five `activation_*` fields, `queue_frames`, `chunk_size`, `max_reorder_frames`, `max_reorder_bytes`, `leg1_replay_bytes`, `leg1_replay_timeout` and `bandwidth_mbps`. The old frame, reorder, replay and weight names are also ignored with a warning inside a direction object. Move direction settings into the client's `upload` / `download` and remove them from the server.

| Old field | Current configuration |
| --- | --- |
| `chunk_size` | Client top-level `frame_size` |
| `leg1_replay_bytes` | Directional `send_buffer_bytes`, shared by both paths |
| `max_reorder_bytes` | Directional `receive_window_bytes`, enforced by that direction's receiver |
| `leg1_replay_timeout` | Directional `path_stall_timeout_min`, applicable to either leg |
| `bandwidth_mbps` | Remove; scheduling is automatic, with no replacement weight or rate cap |
| `max_reorder_frames` | Remove; there is no `receive_window_frames` option |

Unknown fields and invalid new-field values are errors. Remove server-side `failover_enabled` as well.

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
