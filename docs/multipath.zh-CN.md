# Multipath 机制说明

[English](multipath.md) · [配置参考](../README.md#中文)

本文说明 Multipath（协议 v13，beta10）的数据传输、调度、内存、连接生命周期和状态统计。部署示例与完整配置字段见 README。

## 作用范围与 MPTCP 的关系

Multipath 在客户端 outbound 与服务端 inbound 之间组合恰好两条可靠 child 字节流；每条逻辑连接在服务端对应一条访问目标的 TCP 连接。中间节点只需把普通流量转发到聚合监听端口，无需理解 Multipath 协议。

MPTCP 在多条 TCP subflow 上向应用提供一条有序字节流，以连接级数据序号、Data ACK 和路径映射维护整体顺序。Multipath 借鉴这些连接级机制，包括 msk 级发送缓冲、最早完成优先（ECF）调度、机会性重注入与惩罚，以及按序 FIN，但使用代理字节流上的独立分帧协议，不使用 TCP option，也不与 MPTCP 线协议互通。参见 [RFC 8684](https://www.rfc-editor.org/rfc/rfc8684.html)。

TCP/QUIC 的报文拥塞控制、pacing 与重传仍由 child 承担，包括 Hysteria2 使用的拥塞控制。本项目没有实现 MPTCP 的地址发现、回退普通 TCP 或 subflow 耦合拥塞控制。调度设计参考[固定提交的 Linux MPTCP 实现](https://github.com/torvalds/linux/blob/587858367581b9c55c3690f4e63382ad622719d4/net/mptcp/protocol.c)，并不声称跟随所有后续内核变化。

## 认证与访问控制

Multipath 不加密数据；加密由 child 协议或私有链路负责。监听端口若不加保护，任何能访问它的人都能借服务端访问任意目标。

- **`psk`**：两端配置相同的预共享密钥后，每个 hello 都带 HMAC-SHA256 签名，覆盖会话 ID、策略、目标地址、时间戳和随机 nonce。服务端拒绝签名错误、时间偏差超过 2 分钟或 nonce 重放的 hello。PSK 只保护建连，不加密后续数据。
- **`allowed_ips`**（仅服务端）：只接受来自这些前缀（或单个地址）的 child 连接和故障接管 UDP 中继报文，按服务端看到的源地址判断。其他来源的连接在读取 hello 之前就被关闭，报文直接丢弃。
- 服务端既没有 `psk` 和 `allowed_ips`、又监听在非私有地址时，启动日志会给出警告。

双端必须都使用协议 v13。版本不一致时，服务端回复带版本号的明确拒绝，客户端日志会说明原因，不再表现为莫名的连接失败。其他拒绝原因（认证失败、参数不一致、会话已关闭等）同样带原因码。

## 会话与 leg

两条 leg 地位对等：WINDOW 反馈、DATA_FIN、session-close、reset 和 sender status 都可以在任一已就绪的 leg 上收发，**没有哪条 leg 是会话必需的**。leg0 仍是首选路径：常规建连、激活前的应用数据都走它。

- **建立**：客户端先经首选 leg 拨号并创建会话；该 leg 在 `handshake_timeout` 的一半内无法到达服务端时，改由另一条 leg 创建。会话建立后，客户端为每条 leg 维持一个连接管理器。
- **重连**：任一 leg 断开后按指数退避重拨（1 秒起，翻倍到 30 秒；连续稳定 30 秒后退避清零），以 join 方式重新挂入原会话，服务端不会重新拨号目标。
- **失效的 leg**：交付停滞的 leg 不再分配新数据；连续 15 秒没有任何进展、且另一条 leg 可用时，关闭它以便重拨。唯一可用的 leg 不会被这样关闭。
- **leg0 丢失**：新数据转到 leg1，反馈、FIN 和关闭都可经 leg1 完成，不需要开启故障接管或聚合。2 秒的 leg0 中断在实测中保持约 950 Mbps 走 leg1。
- **全部 leg 缺失**：未开启故障接管时，两条 leg 都缺失超过 `max(30 秒, 3 × handshake_timeout)` 后结束会话。开启故障接管时，由恢复组租期和所有权确认决定（见下文）。
- **墓碑**：服务端记住已关闭的会话 ID 两分钟，拒绝迟到的 join 或重复创建，避免对同一 ID 第二次拨号目标。

反馈 WINDOW 帧走写入阻塞最轻的 leg，另一条 leg 至少每 20 ms 收到一份副本；选中的 leg 写入阻塞超过 `max(10 ms, SRTT/2)` 时立即在另一条 leg 上补发。各份副本带序号，单调字段（累计确认、路径回执）取最大值合并，窗口上限与标志位只采用最新序号。应节点要求复查接收窗口的反馈只走控制 leg。DATA_FIN 在其所在 leg 消失或一个 RTO 内未确认时，改在另一条 leg 重发。

## 激活

上传和下载分别配置、分别激活。`aggregation_enabled` 是总开关；开启时，以下条件按每连接、每发送方向独立判断，任一满足即激活（“或”关系）：

1. **排队条件**（`activation_on_queue`，默认开启）：本地未发送字节持续 `activation_window`（默认 200 ms）不低于当前未发送上限的一半，**并且** leg0 的交付速率已不再增长（连续两个 RTT 增幅不足 25%，与 BBR 判断管道已满的方式相同）。前者说明应用快于 leg0，后者说明 leg0 已走出慢启动、确实是瓶颈。
2. **速率条件**（`activation_threshold_mbps`，默认 0 即关闭）：窗口内平均接收应用数据的速率达到阈值。
3. **字节条件**（`activation_after_bytes`，默认关闭）：累计接收应用字节达到阈值；若 `activation_after_bytes_min_mbps` 非零，还须满足完整采样窗口内的最低平均速率。

leg0 交付速率的平台期判断，避免了高 RTT 路径上中等大小传输在 leg0 慢启动阶段就拉入 leg1：此时 leg1 的新 child 连接同样处于慢启动，分给它的靠前字节往往比 leg0 晚到，反而拖慢完成时间。低 RTT 路径上 leg0 的慢启动只需几十毫秒，激活时间基本由 `activation_window` 决定。

三个条件全部关闭就不会激活；数值 0 不代表立即激活。激活后不会因流量减少退回。需要 beta10 之前的行为，可显式写 `activation_threshold_mbps: 150` 与 `activation_window: "1s"`。

## 调度

### 最早完成优先

激活后，每个新数据段交给预计最早送达的路径。路径的预计完成时间为：

`(该路径在途字节 + 本段长度) / 该路径交付速率 + 最小 RTT / 2`

交付速率与 RTT 来自远端 Multipath 接收端的路径回执，因此在途量包括本地代理和远端传输内部的缓冲；本地 socket `Write` 完成不等于交付完成。没有手工带宽权重或限速参数。

- 最小 RTT 10 秒内没有再次出现就失效（与 BBR 相同），路径传播时延变大后估计随之更新。
- 速率样本来自突发到达，看不到丢包或窗口受限的 child 让排队数据多等的整轮往返；因此已有数据在途的路径，其完成时间至少取最近帧实际经历的发送到到达时延（平滑 RTT 减去回执的回程）。
- BLEST 窗口检查：较慢路径上的数据段到达之前，接收端无法越过它交付；只有当接收窗口装得下最佳路径在此期间发出的数据时，较慢路径才接收该数据段，否则最佳路径会被窗口卡住。

- 应用仍有积压（写入被阻塞，或刚刚被阻塞）时，写入空闲的较慢路径也接收数据，使每条路径都保持忙碌，与 MPTCP 默认调度器相同。只保留两项限制：BLEST 窗口检查，以及时差限制——交给较慢路径的数据段，最多比最佳路径晚该路径 8 个传播往返到达。有损的 QUIC child 上数据要等几轮重传、但路径仍有余量，因此不会被饿死；丢包后窗口塌缩的 TCP child 也不能把数秒的数据压在发送缓冲里、卡住按序交付。
- 应用已结束（DATA_FIN 已排队）或暂时没有更多数据时，改用严格 ECF：较慢路径只接收它能在最佳路径送完全部待发数据之前送达的数据段，避免传输尾部落在慢路径上。
- 尚无速率样本的路径借用已测路径中最好的速率和最长的时延，并在第一个样本前最多持有 4 帧。
- 空闲超过 `max(2 × SRTT, 100 ms)` 的路径按乐观速率重新探测；空闲之后、或至少一秒（且超过 RTO）毫无交付的中断之后的第一个回执重新开始速率采样，不把间隔算进速率。这个回执仍会把速率至少抬到“所含字节 / 发送到回执的时间”，因此速率估计崩溃后只能逐帧探测的路径也能重新测出速率。

### 修复与重注入

所有重传都复用同一份连接级发送历史，不消耗新的连接序号或接收窗口，也不复制载荷；迟到的原始数据作为重复范围丢弃，不会重复交付。

- **接收端丢弃修复**：接收端因内存不足拒收数据时，在 WINDOW 帧中报告最多 4 个丢弃区间（不跨越空洞合并）。发送端在一个 RTO 内对同一区间至多修复一次，修复流量不超过每 RTT 交付速率的四分之一。
- **失效路径修复**：leg 断开（实例变化）或停滞时，其上尚未确认的映射在其他可用路径上重发。
- **机会性重注入与惩罚**：新数据因接收窗口或发送历史受阻时，若队头附近的数据所在路径已有一个自身 RTT（且不少于快路径 RTT 两倍）没有任何交付，或这些数据在该路径上预计晚于空闲路径现在重发的到达时间，就在空闲路径上按序重发，并惩罚慢路径一个 RTT 不接收新数据：没有交付时用它的平滑 RTT，否则用传播 RTT（有损路径的平滑 RTT 含重传等待）。
- **尾部重注入**：没有新数据可发时，若队头附近的数据仍排在较慢或刚起步的路径上、预计晚于空闲快路径现在重发的到达时间，就在快路径上再发一份，先到者生效。慢启动中的 child 连接把数据保留数个 RTT 的情形由此覆盖。

### 停滞检测

无进展检测时限为平滑交付 RTT 加四倍时延变化量，至少 200 ms；尚无样本时为 1 秒。`path_stall_timeout_min` 只能再提高这个下限，不是固定重传周期。停滞的路径暂停接收新数据，收到新进展后恢复；停滞本身不要求重连。

## 字节流、发送历史与接收窗口

两条路径的数据映射到同一个 64 位连接级字节序号空间，每条路径另有独立序号和路径实例标识。接收端处理重复和重叠映射，对已经连续收到的前缀返回累计 Data ACK。它表示接收端已接管这些字节，不表示目标应用已经读取。

`frame_size` 是单帧最大载荷，不是固定大小记录。接收端增量处理帧内数据：前缀到达且逻辑连续时即可交付和确认。但更早的连接级字节缺失时，后续字节仍必须等待；按字节处理并没有消除有序流的队头阻塞。

| 状态 | 含义与上限 |
| --- | --- |
| 本地未发送数据 | 已被 MP 接收、尚未分配给路径的字节。上限约为承载路径交付速率 × 10 ms，至少 1 MiB（或 4 帧），至多 `queue_frames × frame_size`，类似 MPTCP 的 `notsent_lowat`。 |
| 路径在途数据 | 已分配给某 leg、尚未得到该路径完整交付回执的字节，包括 child 内部缓冲。 |
| 连接发送历史 | 尚未被累计 Data ACK 确认的全部数据，含未发送部分，类似 MPTCP 的 msk 发送缓冲。目标为承载路径带宽时延积的两倍加未发送上限，至少 4 MiB；发送受历史而非网络限制时每 RTT 增长 25%，需求下降后逐渐回收；至多 `send_buffer_bytes`，并受本会话在发送区内的公平份额约束。 |
| 接收窗口 | 接收端为两条路径通告的同一个连接级字节窗口，至多 `receive_window_bytes`，不小于一帧（发送端在收到任何反馈前可用的额度）。窗口上限可以缩小：发送端按最新上限停止发送新数据，在得知缩小前已发出的数据只要不超过历史最高上限仍会被接收。首次通告前为 256 KiB，大小规则见下文。应用读取推动窗口前进。 |
| 接收存储 | 实际到达的乱序数据和按序但尚未被应用读取的数据，按 16 KiB 页懒分配并复用。 |

`queue_frames`、`send_buffer_bytes` 与 `receive_window_bytes` 都是**上限**，不是容量或预分配；实际值随路径速率、RTT 和内存份额自动变化。默认值即推荐值，通常无需调整。

应用读写直接在接收页与发送缓冲上进行，没有中转管道或中转 goroutine；每个数据帧的帧头与载荷合并为一次 child 写入。

## 内存与反压

每个 Multipath inbound/outbound 使用一份本机预算，覆盖发送载荷、接收页、元数据、缓存与会话固定开销。默认 `min(512 MiB, 可用内存 × 0.5)`；Linux 上取 `MemAvailable` 与可见 cgroup v1/v2 剩余额度的较小值。显式预算不会自动缩小。

预算的 1/16 是应急余量，其余为共享池。池内分三本账：发送载荷（tx）、接收页（rx）和其他（会话预留、UDP 重组等）。

- 发送与接收互相借用对方未用的部分，但始终给对方留出保底：对方方向有活跃会话时为池的一半，否则为 1/8；另外各留 1/16 的余量带，避免一侧把池用满后另一侧无法起步。
- 发送历史在发送区的活跃发送会话之间按 max-min 公平分配：写入没有被阻塞且占用少于平均份额的会话保留已占用的部分，其余平均分给其他会话。最近 1 秒内持有待发送数据或写入被阻塞的会话才算活跃发送方；空闲方向不算，所以只下载的节点把内存池留给接收。
- 接收窗口是承诺：发送端可以填满整个窗口，且经不同路径乱序到达。所以每个会话（无论是否空闲）都按已开放的窗口占用接收区的 3/4（每个会话的第一帧不计），所有会话的窗口合计不超过该区间：
  - 会话的首个窗口为“其他会话未占用部分”的一半，新连接上的响应不会多等一个往返；但至少为接收区的 1/128（且不小于 256 KiB），这部分可以用预留的 1/4，所以同时建立的连接、或已有传输占满接收区时的新连接，不会只剩一帧。有其他传输正在增长时，新连接只拿这个小窗口。
  - 窗口限制发送端时（未读数据占满窗口的 3/4，或窗口小于最近约 1 秒到达量的 4/3：受窗口限制的传输每个往返送达一个窗口），窗口增长到 max-min 份额，但只能占用其他会话没有占用的部分。
  - 否则窗口保持现有大小（不超过份额），直到需要内存。有传输等待内存时，要求高于份额的会话、以及发送端并不需要其窗口的会话复查；新连接的首个窗口放不下时，要求后者中窗口已开放满 1 秒的会话复查。被要求的会话立即把上限降到实际用量（约 1 秒到达量的 4/3，按至少 100 ms 外推），空闲连接降到一帧；还没收到任何数据的新连接保留窗口 1 秒，因为它可能正在等第一个响应。内存充足时，空闲连接恢复传输不会被拖慢。
  - 因此几十条保活连接旁边的一个大流量传输几乎可以用满整个区域，多个都需要更多的传输则平分。接收区预留的 1/4 吸收页面开销，以及发送端得知上限缩小前已发出的数据。
  - 收发两区如何划分只看最近 1 秒收到过数据的会话；空闲方向上的周期性反馈（开启 failover 时每秒一次）不算。
- 接收页满时，靠近下一个期望字节（16 页以内）的数据仍会接收，并挤出最远的未确认页；被拒收或挤出的范围通过 WINDOW 帧报告给发送端修复。
- 接收端已拒收数据、或剩余空间不足两帧时，反馈中带“已满”标志：发送端暂停新数据（最多保留两帧在途），只修复缺口，等接收端报告有空间后恢复。
- 已经 Data ACK、等待应用读取的字节永远不会被丢弃。

“压力”只表示应急余量正在使用：它会记录日志和状态，但**不会关闭 leg1、不会冻结接收窗口，也不改变任何会话的路径选择**。

每个获准会话都预留 reader 工作缓冲、一个队头接收页和一个可复用的发送缓冲，保证队头推进不依赖其他会话先释放内存。缓冲归还预算意味着可以复用或被 GC 回收，不意味着 Go 立即把物理页交回操作系统。

1 GiB 主机可以从 `"memory_limit": "256MB"` 起步，为 child 协议、其他进程和操作系统留出余量。多个 MP 实例各有独立预算，大型 QUIC 窗口还会占用预算外内存。

如果没有设置 `GOMEMLIMIT`，也没有已有的 runtime 内存限制（包括 sing-box 的 `debug.memory_limit`），第一个 MP 实例启动时把进程级 Go 软限制设为“当前 Go 内存占用 + 检测到的可用内存 × 80%”。所有 MP 实例共享这一软限制；最后一个实例停止时恢复原设置。显式 `GOMEMLIMIT`（包括 `off`）优先。它不是 RSS 硬上限；参见 [Go GC 指南](https://go.dev/doc/gc-guide#Memory_limit)。

## 流量节省模式

开启 `leg0_traffic_saving` 时，激活后新数据改由 leg1 独占发送，而非双路径聚合。激活前和 leg1 尚未就绪时仍使用 leg0。健康 leg1 的写入反压会导致等待，不会因此把新数据溢出到 leg0；leg1 缺失或停滞时允许 leg0 回退，leg1 恢复后继续独占新数据。leg1 健康时，尾部重注入也不使用 leg0。已经分配给 leg0 的数据正常完成，控制帧和失效修复仍可能使用 leg0，因此该模式不保证 leg0 零流量。

## 可选路径故障接管

`failover_enabled` 只在客户端配置，默认 false。没有它时，会话本身已能在任一 leg 上存活（见上文）；故障接管额外提供：共享健康检查、新会话直接选择健康路径、UDP 中继与 UDP 路径切换，以及跨长时间双路径中断的会话保留。服务端始终监听 TCP 和 UDP，只有客户端请求时才创建恢复组。

开启后，每个 outbound 为每个 child 维持一条共享 TCP 控制连接和一条原生 UDP 关联，供所有业务连接共用。路径必须同时具有新鲜的 TCP 和 UDP 挑战响应才算健康；完整的 `failover_timeout`（默认 5 秒）无有效响应后判为不可用。探测通常每秒一次；检测与会话重连还会增加时间，配置的 timeout 不是业务中断时间上界。

首选路径第一次确认健康时立即选用。它一旦经历“健康后又故障”，再次恢复须持续健康一段时间才回到正常角色：首次为 3 秒，之后每次反复故障翻倍，最长到 `failback_delay`（默认 30 秒）。单次漏探测不重置稳定计时，完整失效超时才会重置；备用路径也失效时可立即选用健康的首选路径。TCP 和 UDP 分别按各自首选路径执行。

启用接管时，`udp_outbound` 是独立于 TCP leg0 的 UDP 首选路径，必须为两个 child 之一。UDP 从第一个包起就通过服务端中继，切换路径时保留面向目标的 socket 和源端口。服务端按客户端同步的路径 epoch 发送响应，迟到旧消息不能撤销更新的选路。

UDP 保持不可靠报文语义，不走 UDP-over-TCP。中继将大包拆成较小的外层 datagram，独立重组每个报文，不做 MP 层重传；缺片五秒后过期。每个关联最多保留四个未拼完的报文，新的分片报文会挤掉最早的一个，单片即可装下的报文不用等待，因此丢失的分片不会阻塞后续报文。缓存计入共享内存预算，压力下丢弃 UDP 包而非积累可靠队列。五分钟没有应用报文的 UDP 关联过期。恢复组在没有控制或 UDP 流量达到 `max(2 分钟, 4 × failover_timeout + failback_delay)` 后过期。

恢复控制心跳同时确认 TCP 会话所有权。服务端每轮最多询问 64 个 ID，轮询覆盖活跃会话；客户端报告已经不再持有的 ID，只有明确的“已不存在”才释放对应服务端会话。客户端在第一次 hello（包括 fast open）发出前登记所有权，逻辑连接终止后才注销；客户端仍持有的会话不会仅因两条数据 leg 暂时中断而被回收。

两个 child 都须能够访问服务端端口的 TCP 和 UDP。故障接管无法阻止游戏自身超时造成的断线，也不能在服务端重启后保留原 socket。

## 连接关闭

DATA_FIN 占用每个发送方向末尾的一个字节序号。只有其之前的数据全部到达，累计 Data ACK 才覆盖 FIN。应用关闭后拒绝新的本地 I/O，但后台继续排空已接收 TX；对端确认最终序号后才发送 session-close。单方向半关闭不会关闭反向数据流。

如果 FIN 及其之前的数据已经到达，后续 session-close 或路径失败不会丢弃已缓冲 RX，应用可以继续读完。错误、reset 和服务停止仍可以立即中止；本地应用关闭也可以丢弃自身未读 RX。

应用完整 `Close` 后，连续两分钟没有累计 Data ACK 进展会结束剩余会话，避免所有路径都不可用时无限等待 FIN/ACK。这个时限不适用于 `CloseWrite`、`CloseRead` 或仍开放的空闲连接。

应用读写 deadline 覆盖首次 fast-open 写入等待。写入超时返回已接受的前缀，已接受字节继续排队，调用者可以延长 deadline 后续写剩余部分。没有收到 FIN 就终止的不完整流返回错误，而不是干净 EOF。

## 运行时统计（状态 schema 5）

客户端设置 `status_file` 后，会在握手中请求服务端发送 sender-status 帧（schema 4，372 字节），经任一 leg 返回。它按逻辑会话报告服务端下行的队列、发送历史、实时上限、修复与重注入、写阻塞和内存压力。状态更新会合并，不占用 DATA 序号或载荷内存预算。客户端在更新停止三秒后把远端数据标为 stale，不把缺失当作零。

| 位置 | 主要字段 |
| --- | --- |
| `node.memory` | `tx_bytes`、`rx_bytes`、`reserved_bytes`、`cached_bytes`、`active_senders`、`active_receivers`、`receive_share_bytes`、`pressure_threshold_bytes` 及各自峰值 |
| `logical.local_sender` / `remote_sender` | `unsent_limit_bytes`、`history_limit_bytes`（开放会话中的最大当前值）、`replay_bytes`、`fallback_*`（修复）、`opportunistic_reinjections`、`tail_reinjections`、`tail_reinjection_bytes`、反压次数与时长 |
| 每条腿 | `pipeline_limit_bytes` 与 `outstanding_bytes`（本端发送），`remote_pipeline_limit_bytes` 与 `remote_outstanding_bytes`（对端发送），`feedback_frames_sent/received`、`join_count`、交付速率与 RTT |

已有流量的路径独立于 `status_file` 发送低频 PING/PONG，用于观测。RTT 是有效的应用层往返时间，包含代理和传输排队。探测超时、停滞和重注入计数都是 MP 可见事件，不是底层丢包率。速率峰值是启动以来最高的一秒平均值。远端调度速率估计不是当前一秒吞吐或物理带宽。

Leg join 统计已成功挂接的传输（含重连）；join、拨号尝试和远端失败保留已关闭连接的累计值。错误事件携带 `last_error_source`：`local_endpoint`、`remote_endpoint`、`transport`、`shutdown` 或 `unknown`，只用于诊断，不改变调度或关闭时机。

每个 MP inbound/outbound 启动时打印实际预算、应急余量和缓存上限；进入和退出压力状态时各打印一次日志。

## 源码索引

| 内容 | 源码 |
| --- | --- |
| 客户端方向策略及校验 | [policy.go](../protocol/multipath/policy.go)、[option/multipath.go](../option/multipath.go) |
| 握手、认证与版本拒绝 | [protocol.go](../protocol/multipath/protocol.go)、[inbound.go](../protocol/multipath/inbound.go) |
| 会话建立与 leg 管理 | [recovery_tcp.go](../protocol/multipath/recovery_tcp.go)、[outbound.go](../protocol/multipath/outbound.go) |
| 激活 | [activation.go](../protocol/multipath/activation.go) |
| 调度、反馈与重注入 | [scheduler.go](../protocol/multipath/scheduler.go)、[stream/path.go](../protocol/multipath/stream/path.go) |
| 发送历史与接收存储 | [stream/send.go](../protocol/multipath/stream/send.go)、[stream/receive.go](../protocol/multipath/stream/receive.go)、[core.go](../protocol/multipath/core.go) |
| 应用读写 | [logical_conn.go](../protocol/multipath/logical_conn.go) |
| 共享预算与 Go 内存限制 | [memory.go](../protocol/multipath/memory.go)、[memory_available_linux.go](../protocol/multipath/memory_available_linux.go)、[memory_runtime.go](../protocol/multipath/memory_runtime.go) |
| 故障接管与会话所有权 | [recovery_client.go](../protocol/multipath/recovery_client.go)、[recovery_policy.go](../protocol/multipath/recovery_policy.go)、[recovery_sessions.go](../protocol/multipath/recovery_sessions.go) |
| 关闭与半关闭 | [lifecycle.go](../protocol/multipath/lifecycle.go) |
| 远端遥测与客户端状态 | [telemetry.go](../protocol/multipath/telemetry.go)、[status.go](../protocol/multipath/status.go) |

[beta5 设计](development/multipath-beta5.zh-CN.md)和[验证记录](development/multipath-beta5-validation.zh-CN.md)保留为历史开发文档，不是当前配置参考。
