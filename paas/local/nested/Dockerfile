# Copyright 2026 Thousand Birds Inc.
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# A kind node image patched to run nested inside a cgroup-v1 microVM:
#  - a kubelet ExecStartPre shim pre-creates cpuset/hugetlb cgroup slices,
#  - containerd uses the cgroupfs driver (matched by the kind KubeletConfiguration),
#  - restrict_oom_score_adj keeps runc from setting a negative oom_score_adj
#    (which the sandbox denies, otherwise pod sandboxes fail to start).
ARG NODE_IMAGE=docker.io/kindest/node:v1.32.2
FROM ${NODE_IMAGE}
COPY kind-cgroupv1-shim /usr/local/bin/kind-cgroupv1-shim
COPY 05-cgroupv1-shim.conf /etc/systemd/system/kubelet.service.d/05-cgroupv1-shim.conf
RUN chmod +x /usr/local/bin/kind-cgroupv1-shim \
 && sed -i 's/SystemdCgroup = true/SystemdCgroup = false/g' /etc/containerd/config.toml \
 && sed -i 's/restrict_oom_score_adj = false/restrict_oom_score_adj = true/g' /etc/containerd/config.toml
