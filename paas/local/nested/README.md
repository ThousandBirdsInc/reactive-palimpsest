# Nested kind (cgroup-v1 microVM)

Scaffolding for running the local kind cluster **nested inside a cgroup-v1
microVM** — specifically the Claude Code web/sandbox environment, whose kernel
boots in cgroup v1 (hybrid) mode with IPv6 disabled and routes egress through a
TLS-intercepting proxy. On a normal cgroup-v2 dev machine none of this is
needed; just run `local/up.sh` without `PALIMPSEST_PAAS_KIND_NESTED`.

Enable with:

```sh
PALIMPSEST_PAAS_KIND_NESTED=1 ./local/up.sh
```

## What each piece works around

| File | Problem it solves |
|------|-------------------|
| `prepare.sh` (host `name=systemd`/`cpuset`/`hugetlb` mounts) | A private cgroup namespace can't *create* these v1 controllers under the sandbox, but it can *attach* to ones already mounted on the host. Without them the node's systemd / kubelet refuse to start. |
| `Dockerfile` + `kind-cgroupv1-shim` | The kubelet's cgroupfs driver needs `cpuset`/`hugetlb` slice paths that systemd doesn't delegate on cgroup v1; the shim pre-creates them as a kubelet `ExecStartPre`. |
| `Dockerfile` (`SystemdCgroup = false`) + `kind-config.yaml` (`cgroupDriver: cgroupfs`) | Avoids the systemd cgroup driver, which can't manage `cpuset`/`hugetlb` on cgroup v1. |
| `Dockerfile` (`restrict_oom_score_adj = true`) | The sandbox denies writing a negative `oom_score_adj`; without this, runc fails to start every pod sandbox (`can't get final child's PID from pipe: EOF`). |
| `kind-config.yaml` (`ipFamily: ipv4`) + IPv4-only podman network | The kernel has IPv6 disabled, so dual-stack networking fails. |
| `inject-host-cas.sh` | containerd inside the node must trust the host's egress-proxy CA to pull images. |

## Caveat: not durable across idle

The host cgroup mounts (`prepare.sh`) do not survive the microVM being
reclaimed after inactivity, and the node container can wedge across long idle
gaps. Re-running `PALIMPSEST_PAAS_KIND_NESTED=1 ./local/up.sh` re-establishes
the mounts and recreates the cluster.
