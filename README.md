# GRIMOIRE — 分布式可扩展游戏服务器（练手项目）

用 Rust 实现的一个分布式游戏服务器骨架：**一套共享网络/服务框架 + 三种玩法服务**。
刻意让三种玩法采用不同的通信范式，用来对比"分布式游戏服务器的不同流派"。

## 架构

```
客户端 / 压测 sim (grimoire-sim)
   │  TCP 长连接 + 自定义帧协议 (protobuf payload)
   ▼
[gateway 网关]            ←→  连接管理 / 帧解析 / 按 msg_id 玩法域路由
   │  gRPC (tonic)
   ▼
[registry 注册中心]       ←→  服务注册 / 心跳续约 / TTL 过期 / 发现（自研，etcd 语义）
   │
   ├── room-svc    MMO 大厅/房间制
   ├── battle-svc  实时对战（帧同步）
   └── card-svc    卡牌回合制
```

**帧协议**（`crates/grimoire-net`）：`magic(2) + ver(1) + ptype(1) + msg_id(4) + seq(4) + len(4) + payload`
- `msg_id` 高 8 位 = 玩法域（1 room / 2 battle / 3 card），低 24 位 = 域内消息号
- `ptype`: 0=request 1=response 2=push 3=heartbeat 4=close
- gateway **只按玩法域路由**，域内分发由各业务服务自己处理 → 新增玩法只需注册新域

## 快速开始

```bash
cargo build --release          # 编译
bash scripts/run-all.sh        # 启动 registry/gateway/三个服务
./target/release/grimoire-sim --mode room     # MMO 房间制 demo
./target/release/grimoire-sim --mode battle   # 实时对战帧同步 demo
./target/release/grimoire-sim --mode card     # 卡牌回合制 demo
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
├── net/       # TCP 帧编解码（粘包拆包、心跳）
├── pb/        # protobuf + gRPC 生成代码（prost/tonic，vendored protoc）
├── common/    # msg_id 分配表、服务名常量
├── svcfw/     # 服务框架：注册/心跳、gateway push 客户端
├── registry/  # 注册中心（内存实现：注册/心跳/TTL/发现）
├── gateway/   # 网关（TCP 接入 + 业务桥接 gRPC + push/kick）
├── room-svc/  # MMO 大厅/房间制
├── battle-svc # 实时对战帧同步
├── card-svc/  # 卡牌回合制
└── sim/       # 测试/压测客户端
```

## 各玩法实现要点

- **room-svc**：`DashMap` 存玩家/房间；登录→建房→入房→聊天全程 push 广播；`PlayerDisconnected` 自动离房清人。注意 DashMap 的 `get()` Ref 未释放就 `get_mut()` 会自死锁。
- **battle-svc**：`tokio::time::interval(50ms)` 全局模拟节拍；每帧按玩家最近输入做确定性位移模拟，向战斗内所有连接广播同一份 `FrameSyncPush`。客户端看到的第 N 帧与所有人完全一致。
- **card-svc**：`phase + turn` 状态机；出牌/结束回合全部服务端校验（回合、手牌序号、目标合法性）；快照按接收者视角裁剪（自己见手牌明细、对手只见数量）。

## 压测对比

| 构建 | 并发连接 | QPS | p50 | p99 |
|---|---|---|---|---|
| debug | 200 | ~2.3k | 82ms | 139ms |
| release | 200 | ~20.6k | 9.4ms | 17ms |
| release | 500 | ~23.7k | 20.6ms | 34ms |

## 已知简化 & 后续路线

- 注册中心为单节点内存版（接口对齐 etcd，可替换为真实 etcd）
- 实时对战未做 UDP/KCP、延迟补偿、快照回放（可参考 `battle-svc` 继续扩展）
- 无持久化（可接 Postgres + Redis，`docker-compose` 已预留）
- 无鉴权/TLS/网关集群化
- 正式项目应全异步化锁（当前 demo 对房间/战斗用了 await 锁，可换无锁结构）
