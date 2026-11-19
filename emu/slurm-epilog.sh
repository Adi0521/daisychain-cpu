#!/bin/bash
# Slurm epilog for emulated workers (runs as root after each job on each node).
#
# Inside a container the cgroup namespace root is not the real root, so slurmstepd cannot
# move itself back there at exit ("no internal processes" rule) and leaves its job cgroup
# directories behind. They hold no processes, so remove every job cgroup that is now
# empty. rmdir fails on a cgroup that still has live processes or children, so running
# jobs are never touched. Real hardware with systemd does not need this.
scope=/sys/fs/cgroup/system.slice/$(hostname)_slurmstepd.scope
[ -d "$scope" ] || exit 0
shopt -s nullglob
# Depth-first: children first, then the directory itself. Globbing rather than find,
# which does not reliably walk cgroupfs here.
rmtree() {
  local d
  for d in "$1"/*/; do rmtree "${d%/}"; done
  rmdir "$1" 2>/dev/null
}
for job in "$scope"/job_*; do rmtree "$job"; done
exit 0
