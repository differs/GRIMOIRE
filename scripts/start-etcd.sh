#!/bin/bash
# 启动 etcd 容器（GRIMOIRE 注册中心后端）
set -e
docker rm -f grimoire-etcd 2>/dev/null || true
docker run -d --name grimoire-etcd --network host \
  quay.io/coreos/etcd:v3.5.4 \
  /usr/local/bin/etcd --name etcd1 --data-dir /etcd-data \
  --listen-client-urls http://127.0.0.1:2379 \
  --advertise-client-urls http://127.0.0.1:2379 \
  --listen-peer-urls http://127.0.0.1:2380 \
  --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --initial-cluster etcd1=http://127.0.0.1:2380
sleep 2
echo "etcd up: http://127.0.0.1:2379"
