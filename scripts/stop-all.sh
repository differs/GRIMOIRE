#!/bin/bash
# 停止全部服务
for p in grimoire-registry grimoire-gateway grimoire-room-svc grimoire-battle-svc grimoire-card-svc; do
  pkill -x "$p" 2>/dev/null
done
echo "all stopped"
