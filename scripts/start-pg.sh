#!/bin/bash
# 启动持久化依赖（Postgres + Redis）
set -e
docker rm -f grimoire-pg grimoire-redis 2>/dev/null || true
docker run -d --name grimoire-pg --network host -e POSTGRES_PASSWORD=grimoire -e POSTGRES_USER=grimoire -e POSTGRES_DB=grimoire postgres:16 >/dev/null
docker run -d --name grimoire-redis --network host redis:7 >/dev/null
sleep 6
echo "postgres: 127.0.0.1:5432 (grimoire/grimoire)"
echo "redis:    127.0.0.1:6379"
