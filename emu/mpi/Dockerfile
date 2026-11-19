# MPI job image for containerised Slurm steps (PRD F6 and the §6 Slurm/container gate).
# Same Ubuntu release and Open MPI/PMIx packages as the nodes (emu/node.Dockerfile), so the
# PMIx client inside the container talks to the same PMIx version Slurm runs on the host.
# Build context: emu/mpi.
FROM ubuntu:24.04 AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc libc6-dev libopenmpi-dev openmpi-bin \
 && rm -rf /var/lib/apt/lists/*
COPY dcbench.c /src/dcbench.c
RUN mpicc -O2 -Wall -Werror -o /usr/local/bin/dcbench /src/dcbench.c

FROM ubuntu:24.04
RUN apt-get update \
 && apt-get install -y --no-install-recommends openmpi-bin \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/dcbench /usr/local/bin/dcbench
# Lets checks prove a rank ran inside this image rather than on the host.
RUN echo mpi > /etc/daisychain-image
LABEL org.daisychain.role=mpi
CMD ["dcbench"]
