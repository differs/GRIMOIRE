#!/bin/bash
# 一键启动全部服务（debug 构建）
set -e
cd "$(dirname "$0")/.."
cargo build 2>/dev/null || cargo build
BIN=target/debug
echo "starting registry...";   $BIN/grimoire-registry   > /tmp/grimoire-registry.log 2>&1 & sleep 0.4
echo "starting gateway...";    $BIN/grimoire-gateway    > /tmp/grimoire-gateway.log  2>&1 & sleep 0.4
echo "starting room-svc...";   $BIN/grimoire-room-svc   > /tmp/grimoire-room.log      2>&1 & sleep 0.2
echo "starting battle-svc..."; $BIN/grimoire-battle-svc > /tmp/grimoire-battle.log    2>&1 & sleep 0.2
echo "starting card-svc...";   $BIN/grimoire-card-svc   > /tmp/grimoire-card.log      2>&1 &
sleep 1
echo "all services up. 使用 ./target/debug/grimoire-sim --mode room|battle|card|bench 测试"
