#!/bin/bash
# SFU 部署前的内核网络参数调优 (需要 root)
#
#   sudo bash scripts/sysctl-sfu.sh
#
# 重点是把 UDP socket 收发缓冲区上限从默认 4MB 调大，
# 否则 settockopt(SO_RCVBUF/SO_SNDBUF) 会被内核截断，高 fanout 时丢包。
set -euo pipefail

# 单 socket 收发缓冲区上限: 64MB (内核会把用户请求值加倍后封顶在此)
sysctl -w net.core.rmem_max=67108864
sysctl -w net.core.wmem_max=67108864
# 新建 socket 的默认收发缓冲区: 4MB
sysctl -w net.core.rmem_default=4194304
sysctl -w net.core.wmem_default=4194304
# 网卡/协议栈收包队列 (高 PPS 场景)
sysctl -w net.core.netdev_max_backlog=250000
sysctl -w net.core.netdev_budget=600
# 全局 UDP 内存页数 (min pressure max)，31GB 内存机器的保守值
sysctl -w net.ipv4.udp_mem="1556480 2075648 3112960"
sysctl -w net.ipv4.udp_rmem_min=16384
sysctl -w net.ipv4.udp_wmem_min=16384
# SO_BUSY_POLL 依赖 (如需在服务端启用忙轮询)
sysctl -w net.core.busy_poll=50
sysctl -w net.core.busy_read=50

# 持久化
cat > /etc/sysctl.d/99-sfu.conf <<'EOF'
net.core.rmem_max=67108864
net.core.wmem_max=67108864
net.core.rmem_default=4194304
net.core.wmem_default=4194304
net.core.netdev_max_backlog=250000
net.core.netdev_budget=600
net.ipv4.udp_mem=1556480 2075648 3112960
net.ipv4.udp_rmem_min=16384
net.ipv4.udp_wmem_min=16384
net.core.busy_poll=50
net.core.busy_read=50
EOF

echo "已应用:"
sysctl net.core.rmem_max net.core.wmem_max net.core.rmem_default net.core.wmem_default \
        net.core.netdev_max_backlog net.ipv4.udp_mem net.core.busy_poll net.core.busy_read
