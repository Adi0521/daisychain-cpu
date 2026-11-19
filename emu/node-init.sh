#!/bin/bash
# Start this node's services. Run via `docker exec` after the network is wired.
#   node-init.sh controller | worker
# Expects /etc/munge/munge.key and /etc/slurm/{slurm,cgroup}.conf to be in place.
set -euo pipefail
role=${1:?role}

chown munge:munge /etc/munge/munge.key
chmod 0400 /etc/munge/munge.key
mkdir -p /run/munge && chown munge:munge /run/munge
pgrep -x munged >/dev/null || runuser -u munge -- munged

if [ "$role" = worker ]; then
  # cgroup v2 forbids enabling controllers for children of a cgroup that has processes
  # ("no internal processes"). Move everything in the container's root cgroup into a
  # leaf, then delegate the controllers Slurm's cgroup/v2 plugin needs.
  cg=/sys/fs/cgroup
  mkdir -p $cg/init
  # Snapshot the list each pass: reading cgroup.procs while moving entries skips some.
  for _ in 1 2 3 4 5; do
    procs=$(cat $cg/cgroup.procs)
    [ -z "$procs" ] && break
    for pid in $procs; do
      echo "$pid" > $cg/init/cgroup.procs 2>/dev/null || true  # exited pids are fine
    done
  done
  echo "+cpuset +cpu +memory +pids" > $cg/cgroup.subtree_control
  # With IgnoreSystemd=yes slurmd creates its scope under system.slice, which systemd
  # would normally provide.
  mkdir -p $cg/system.slice
  echo "+cpuset +cpu +memory +pids" > $cg/system.slice/cgroup.subtree_control
  pgrep -x slurmd >/dev/null || slurmd
else
  pgrep -x slurmctld >/dev/null || slurmctld
fi
