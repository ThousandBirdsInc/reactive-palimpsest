# PaaS Deploy Artifacts

This directory contains owned host deployment artifacts for the managed PaaS
runtime. These files are intentionally separate from the existing standalone
Helm chart and do not use Kubernetes.

## Node Host Systemd Units

The first host deployment slice is under `systemd/`.

Control-plane unit:

- `palimpsest-paas-control-plane.service` runs the SQL control-plane API.
- `palimpsest-paas-control-plane.env.example` documents the metadata database,
  agent-token, envelope-secret, billing export, and backup scheduler settings.

Node-host units:

- `palimpsest-paas-node-agent-register.service` registers a database host with
  the SQL control plane.
- `palimpsest-paas-node-agent-heartbeat.service` records host capacity and
  state.
- `palimpsest-paas-node-agent-heartbeat.timer` runs the heartbeat regularly.
- `palimpsest-paas-node-agent-poll.service` leases and executes one queued
  node-agent command.
- `palimpsest-paas-node-agent-poll.timer` runs the poller regularly.
- `palimpsest-paas-node-agent.env.example` documents required environment.

The units assume the `palimpsest-paas-node-agent` binary is installed at
`/usr/local/bin/palimpsest-paas-node-agent` and run as a dedicated
`palimpsest` user. The runtime root is `/var/lib/palimpsest` by default.

## Bootstrap

`host-images/bootstrap-control-plane-host.sh` creates the host user, control
plane state directory, billing-export directory, configuration directory, and
installs the control-plane systemd unit from this repository checkout.

`host-images/bootstrap-node-host.sh` creates the host user, runtime
directories, configuration directory, and installs the node-agent systemd units
from this repository checkout.

The bootstrap scripts do not install PostgreSQL 18 binaries or Palimpsest
PaaS binaries. Host images should install those before running the bootstrap
script.
