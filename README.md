# GRIMOIRE — 分布式可扩展游戏服务器（练手项目）

用 Rust 实现的一个分布式游戏服务器骨架：**一套共享网络/服务框架 + 三种玩法服务**。
刻意让三种玩法采用不同的通信范式，用来对比"分布式游戏服务器的不同流派"。

## 架构

```
客户端 / 压测 sim (grimoire-sim)
   │  TCP 长连接 + 自定义帧协议 (protobuf payload)
   │  UDP 低延迟通道（实时对战帧同步）
   ▼
[gateway 网关 × N]        ←→  连接管理 / 帧解析 / 按 msg_id 玩法域路由 / push-kick
   │  gRPC (tonic)
   ▼
[registry 注册中心]       ←→  服务注册 / 心跳续约 / 租约过期 / 前缀发现
   │  后端可选：etcd（推荐）或内存
   │
   ├── room-svc    MMO 大厅/房间制
   ├── battle-svc  实时对战（帧同步）
   └── card-svc    卡牌回合制
```

**帧协议**（`crates/grimoire-net`）：`magic(2) + ver(1) + ptype(1) + msg_id(4) + seq(4) + len(4) + payload`
- `msg_id` 高 8 位 = 玩法域（1 room / 2 battle / 3 card），低 24 位 = 域内消息号
- `ptype`: 0=request 1=response 2=push 3=heartbeat 4=close
- gateway **只按玩法域路由**，域内分发由各业务服务自己处理 → 新增玩法只需注册新域

## 核心特性

1. **多活网关**：`conn_id` 高 8 位编码网关 ID；网关自注册到注册中心，业务服务凭 `conn_id` 定位并推送到对应网关。多实例网关各自独立接入，客户端可分散到不同网关（跨网关对局正常）。
2. **etcd 注册中心**：注册中心后端可切 etcd——注册=租约+写 key、心跳=keep_alive 续约、发现=前缀 Range。**进程失联时 etcd 租约自动过期删 key**（无需本地扫描器），接口与内存版完全一致。
3. **UDP 帧同步**：实时对战走 UDP 低延迟通道——客户端 UDP 包首包绑定 `conn_id`，输入经 UDP 上行，帧同步经 UDP 下行；未绑定 UDP 时网关自动回退 TCP。

## 快速开始

```bash
cargo build --release          # 编译
bash scripts/start-etcd.sh     # 启动 etcd（推荐后端）
bash scripts/run-multi.sh      # 启动 registry(etcd)/双网关/三个服务
# 测试（A 走 gw1、B 走 gw2 的跨网关对局）
./target/release/grimoire-sim --mode room       --gateway 127.0.0.1:9000 --gateway-b 127.0.0.1:9001
./target/release/grimoire-sim --mode battle     --gateway 127.0.0.1:9000 --gateway-b 127.0.0.1:9001
./target/release/grimoire-sim --mode battle-udp --gateway 127.0.0.1:9000 --gateway-b 127.0.0.1:9001 --udp-gateway 127.0.0.1:9020
./target/release/grimoire-sim --mode card       --gateway 127.0.0.1:9000 --gateway-b 127.0.0.1:9001
./target/release/grimoire-sim --mode bench --clients 200 --duration 5   # 压测
bash scripts/stop-all.sh
```

单机 debug 压测约 2.3k QPS（p50 82ms），release 约 20k QPS（p50 9ms）。

## 三种玩法的范式对比

| 维度 | room-svc（MMO 大厅/房间） | battle-svc（MOBA/射击） | card-svc（卡牌回合制） |
|---|---|---|---|
| 通信模型 | 请求/响应 + 低频状态广播 | 高频输入上行 + 固定 tick 权威快照下行 | 低频事件上行 + 全量快照下行 |
| 同步方式 | 全量状态，房间内成员共享 RoomInfo | 帧同步：服务端 20Hz 模拟，同帧同状态 | 事件驱动，每次变更推全量快照 |
| 一致性 | 最终一致（状态变化即广播） | 最强：所有端同时收到同一权威帧 | 严格串行：回合状态机 + 服务端校验 |
| 延迟要求 | 宽松（百 ms 级） | 苛刻（<100ms，允许丢中间帧） | 宽松 |
| 状态归属 | 服务端权威全量 | 服务端权威确定性模拟 | 服务端权威 + 视角裁剪（只看到自己的手牌） |
| 扩展方式 | 房间分片（room_id hash 到节点） | 战斗实例无状态、天然可水平扩展 | 单局低频小数据，垂直扩展友好 |
| 失败重放 | 全量重下发即可 | 需要确定性 + 快照回滚 | 事件日志可重放 |

**核心区别一句话**：房间制重状态管理与广播、实时对战重延迟与确定性、卡牌重业务逻辑与校验。

## 代码结构

```
crates/
├── net/          # TCP 帧编解码 + UDP 数据报格式（粘包拆包、心跳）
├── pb/           # protobuf + gRPC 生成代码（prost/tonic，vendored protoc）
├── common/       # msg_id 分配表、服务名常量、全局 conn_id 编码
├── svcfw/        # 服务框架：注册/心跳、按网关发现的 push 客户端
├── registry/     # 注册中心（etcd 后端 + 内存后端双实现）
├── gateway/      # 多活网关（TCP 接入 + UDP 通道 + 业务桥接 gRPC + push/kick）
├── room-svc/     # MMO 大厅/房间制
├── battle-svc/   # 实时对战帧同步（UDP 广播）
├── card-svc/     # 卡牌回合制
└── sim/          # 测试/压测客户端（room/battle/battle-udp/card/bench）
```

## 各玩法实现要点

- **room-svc**：`DashMap` 存玩家/房间；登录→建房→入房→聊天全程 push 广播；`PlayerDisconnected` 自动离房清人。
- **battle-svc**：`tokio::time::interval(50ms)` 全局模拟节拍；每帧按玩家最近输入做确定性位移模拟，向战斗内所有连接广播同一份 `FrameSyncPush`（UDP）。客户端看到的第 N 帧与所有人完全一致。
- **card-svc**：`phase + turn` 状态机；出牌/结束回合全部服务端校验；快照按接收者视角裁剪（自己见手牌明细、对手只见数量）。

## 踩坑记录（DashMap 锁 & 运行时冻结）

DashMap 的 `get()/get_mut()/iter()` 返回的 `Ref` 持有分片**同步读锁**，parking_lot 写优先。两个高危模式会**阻塞整个 tokio worker 线程**，连锁卡死定时器/心跳，进程看起来活着但完全僵死：

1. **自死锁**：Ref 存活期间对同一分片 `remove()/get_mut()`（写锁）——battle/card 的 `leave()`。
2. **锁跨 await 死锁**：持有 A 分片读锁时 `await` 另一把锁，而持后者写锁的路径又在等 A 分片写锁——gateway `resolve()` 的缓存 Ref 跨 `last_fetch.read().await`，与 `fetch()` 的 `last_fetch` 写锁 + cache 写锁互相等待。

修复原则：**任何 await 期间不持有 DashMap Ref / 同步锁**；先用 `clone()/drop()` 取出数据再异步操作。

## 压测对比（release + etcd 注册中心）

| 版本 | 并发连接 | QPS | p50 | p99 |
|---|---|---|---|---|
| 初始（单网关、内存注册） | 200 | ~20.6k | 9.4ms | 17ms |
| 优化前（etcd、双网关） | 200 | ~37.9k | 5.2ms | 8.0ms |
| 连接池+NODELAY | 200 | ~55.3k | 3.6ms | 6.4ms |
| **流式多路复用** | 200 | **~120-154k** | **1.3-1.5ms** | 2.4-3.9ms |
| 流式多路复用 | 1000 | ~160-173k | 5.4-5.8ms | 10-12ms |

单连接本征 RTT 约 130µs（客户端→网关→业务服务→返回，含 gRPC）。

## 性能优化记录（按收益排序）

1. **网关↔服务双向流多路复用**（最大收益，吞吐上限 60k→170k）：每条游戏消息不再是一次 unary RPC（h2 流创建/销毁 + 头处理），而是复用每条 `(玩法域, 节点)` 持久双向流上的一条数据帧；响应按 `(conn_id, seq)` 关联回客户端。
2. **网关→服务 h2 连接池**：单条 h2 连接在高并发 unary RPC 下流锁竞争严重，改为每节点 4 条连接轮询。
3. **TCP_NODELAY**：网关接入 socket 与客户端均关闭 Nagle，避免小包延迟叠加。
4. **发现缓存零克隆**：`Discovery::resolve` 原实现每请求克隆整个节点 Vec（含 String），改为 `Arc<NodeInfo>` 缓存只克隆 Arc。

~170k QPS 后热点已摊平（任务调度/h2 帧/序列化/通道均分），进一步优化需协议层（如批量帧、零拷贝）或减少每请求 spawn/oneshot 链。

## 已知简化 & 后续路线

- 实时对战未做 KCP、延迟补偿、快照回放（已走通 UDP 通道，可继续扩展）
- 无持久化（可接 Postgres + Redis）
- 无鉴权/TLS、网关无连接迁移（客户端掉线后重连可做到无缝重绑）
- 正式项目应全异步化锁（当前用 `tokio::sync::Mutex`，可换无锁结构）
