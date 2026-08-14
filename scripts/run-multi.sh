#!/bin/bash
# 多活网关 + 业务服务 + 注册中心 一键启动（推荐：etcd 注册中心）
# 依赖：docker 中运行着 etcd（scripts/start-etcd.sh），或设置 GRIMOIRE_ETCD 指向已有 etcd
set -e
cd "$(dirname "$0")/.."
cargo build 2>/dev/null || cargo build
BIN=target/debug

# 注册中心：默认 etcd，未检测到则退回内存版
if [ -z "$GRIMOIRE_ETCD" ]; then
    if docker exec grimoire-etcd true 2>/dev/null; then
        GRIMOIRE_ETCD="http://127.0.0.1:2379"
    fi
fi
if [ -n "$GRIMOIRE_ETCD" ]; then
    echo "registry backend: etcd ($GRIMOIRE_ETCD)"
    $BIN/grimoire-registry --registry "$GRIMOIRE_ETCD" > /tmp/grimoire-registry.log 2>&1 &
else
    echo "registry backend: in-memory"
    $BIN/grimoire-registry > /tmp/grimoire-registry.log 2>&1 &
fi
sleep 0.4

echo "starting gateway 1 (id=1) ..."
$BIN/grimoire-gateway --id 1 --client-listen 127.0.0.1:9000 --grpc-listen 127.0.0.1:9100 --udp-listen 127.0.0.1:9020 > /tmp/grimoire-gw1.log 2>&1 &
sleep 0.4
echo "starting gateway 2 (id=2) ..."
$BIN/grimoire-gateway --id 2 --client-listen 127.0.0.1:9001 --grpc-listen 127.0.0.1:9101 --udp-listen 127.0.0.1:9021 > /tmp/grimoire-gw2.log 2>&1 &
sleep 0.4
$BIN/grimoire-room-svc   > /tmp/grimoire-room.log   2>&1 &
sleep 0.2
$BIN/grimoire-battle-svc > /tmp/grimoire-battle.log 2>&1 &
sleep 0.2
$BIN/grimoire-card-svc   > /tmp/grimoire-card.log   2>&1 &
sleep 1
echo "all up."
echo "  gateway1 tcp:9000 grpc:9100 udp:9020 | gateway2 tcp:9001 grpc:9101 udp:9021"
echo "  测试: ./target/debug/grimoire-sim --mode room|battle|battle-udp|card --gateway 127.0.0.1:9000 --gateway-b 127.0.0.1:9001"
