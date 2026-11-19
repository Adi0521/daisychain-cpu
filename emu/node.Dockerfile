# Base image for every emulated node (controller, workers, switches) and the wiring helper.
# Build context is the repository root (see daisychain.emu.build_image).
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      iproute2 iputils-ping iperf3 ethtool tcpdump netcat-openbsd procps psmisc python3 \
      ca-certificates \
      munge slurm-wlm slurm-client \
      nfs-kernel-server nfs-common \
 && rm -rf /var/lib/apt/lists/*

# Apptainer from the project's official PPA, non-setuid build: job containers run as the
# submitting user via user namespaces, with no daemon (PRD §6 rootless preference).
RUN apt-get update \
 && apt-get install -y --no-install-recommends software-properties-common gpg-agent \
 && add-apt-repository -y ppa:apptainer/ppa \
 && apt-get update \
 && apt-get install -y --no-install-recommends apptainer \
 && apt-get purge -y software-properties-common gpg-agent && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

# Unprivileged experiment user, same UID/GID on every node (PRD F1: consistent users).
RUN groupadd -g 2000 dcuser && useradd -m -u 2000 -g 2000 -s /bin/bash dcuser

RUN mkdir -p /etc/slurm /var/spool/slurmctld /var/spool/slurmd /var/log/slurm \
 && chown slurm:slurm /var/spool/slurmctld /var/log/slurm

# Worker-local artifact stores, written by staging jobs running as dcuser (F7).
RUN mkdir -p /var/lib/daisychain/stage /var/lib/daisychain/images /shared \
 && chown dcuser:dcuser /var/lib/daisychain/stage /var/lib/daisychain/images

COPY emu/node-init.sh /usr/local/sbin/node-init.sh
COPY emu/slurm-epilog.sh /etc/slurm/epilog.sh
COPY src/daisychain/stage.py /usr/local/bin/daisychain-stage
RUN chmod 0755 /usr/local/sbin/node-init.sh /etc/slurm/epilog.sh /usr/local/bin/daisychain-stage

CMD ["sleep", "infinity"]
