#!/bin/bash
# Start this node's services. Run via `docker exec` after the network is wired.
#   node-init.sh controller | worker
# Expects /etc/munge/munge.key and /etc/slurm/{slurm,cgroup}.conf to be in place.
set -euo pipefail
role=${1:?role}

# NFS client options: `soft` with a short timeout gives a bounded error (about 15 s)
# instead of hanging forever when the server or a link disappears (PRD §10). That is
# safe here because staging verifies checksums and results are written to a temporary
# name and then renamed.
NFS_OPTS=${NFS_OPTS:-vers=4.2,soft,timeo=50,retrans=2,noatime}
# NFSv4 server lease and grace times (kernel default 90 s each). After any server start or
# restart, opens block for the grace period; 15 s keeps bring-up and failure-recovery
# experiments short. Recorded in docs/emulated-cluster.md; revisit for hardware.
NFS_LEASE_S=${NFS_LEASE_S:-15}
NFS_GRACE_S=${NFS_GRACE_S:-15}
DATA_NET=${DATA_NET:-10.10.0.0/24}

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

  # Shared storage from the controller, over the emulated fabric.
  if ! mountpoint -q /shared; then
    for i in $(seq 1 15); do
      mount -t nfs4 -o "$NFS_OPTS" C0:/ /shared 2>/dev/null && break
      sleep 1
    done
    mountpoint -q /shared || { echo "NFS mount of C0:/ failed" >&2; exit 1; }
  fi
  pgrep -x slurmd >/dev/null || slurmd
else
  # NFSv4-only kernel server exporting /export (an ext4 volume) to the data network.
  # Artifacts are root-owned and read-only to jobs (root is squashed on clients);
  # results/ is writable by the experiment user.
  mkdir -p /export/models /export/images /export/datasets /export/results
  chown dcuser:dcuser /export/results
  mountpoint -q /proc/fs/nfsd || mount -t nfsd nfsd /proc/fs/nfsd
  # Lease/grace can only be changed while nfsd is stopped in this network namespace.
  if [ "$(cat /proc/fs/nfsd/threads)" = 0 ]; then
    echo "$NFS_LEASE_S" > /proc/fs/nfsd/nfsv4leasetime
    echo "$NFS_GRACE_S" > /proc/fs/nfsd/nfsv4gracetime
  fi
  date -u +%FT%TZ > /export/.ready
  echo "/export $DATA_NET(rw,sync,no_subtree_check,root_squash,fsid=0)" > /etc/exports
  exportfs -ra
  pgrep -x rpc.mountd >/dev/null || rpc.mountd -N 2 -N 3
  rpc.nfsd -N 3 8
  pgrep -x slurmctld >/dev/null || slurmctld
fi
