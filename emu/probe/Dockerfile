# Job-container probe for the Slurm/container gate (PRD §6). Deliberately a different OS
# (Debian 12) from the nodes (Ubuntu 24.04), so checks can tell they are inside it.
FROM python:3.12-slim-bookworm
LABEL org.daisychain.role=probe
CMD ["python3", "--version"]
