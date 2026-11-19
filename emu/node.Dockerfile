# Base image for every emulated node (controller, workers, switches) and the wiring helper.
# Later increments add Slurm, MPI, NFS and exporters on top of this layer.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      iproute2 iputils-ping iperf3 ethtool tcpdump netcat-openbsd procps python3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*

CMD ["sleep", "infinity"]
