# Base image for every emulated node (controller, workers, switches) and the wiring helper.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      iproute2 iputils-ping iperf3 ethtool tcpdump netcat-openbsd procps psmisc python3 \
      ca-certificates \
      munge slurm-wlm slurm-client \
 && rm -rf /var/lib/apt/lists/*

# Apptainer from the project's official PPA, non-setuid build: job containers run as the
# submitting user via user namespaces, with no daemon (PRD §6 rootless preference).
RUN apt-get update \
 && apt-get install -y --no-install-recommends software-properties-common gpg-agent \
 && add-apt-repository -y ppa:apptainer/ppa \
 && apt-get update \
 && apt-get install -y --no-install-recommends apptainer \
 && apt-get purge -y software-properties-common gpg-agent && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /var/lib/daisychain/images

# Unprivileged experiment user, same UID/GID on every node (PRD F1: consistent users).
RUN groupadd -g 2000 dcuser && useradd -m -u 2000 -g 2000 -s /bin/bash dcuser

RUN mkdir -p /etc/slurm /var/spool/slurmctld /var/spool/slurmd /var/log/slurm \
 && chown slurm:slurm /var/spool/slurmctld /var/log/slurm

COPY node-init.sh /usr/local/sbin/node-init.sh
COPY slurm-epilog.sh /etc/slurm/epilog.sh
RUN chmod 0755 /usr/local/sbin/node-init.sh /etc/slurm/epilog.sh

CMD ["sleep", "infinity"]
