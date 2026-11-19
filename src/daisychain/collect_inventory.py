#!/usr/bin/env python3
"""Collect a hardware/software inventory of this host as JSON (PRD §14 decision 1, F1).

Standard library only and Python >= 3.8, so it can be copied to a fresh Ubuntu node and
run before anything else is installed:

    python3 collect_inventory.py > inventory/hosts/$(hostname -s).json

Missing tools or permissions produce null fields plus an entry in "warnings"; the script
never guesses a value it could not observe. Non-Linux hosts (e.g. a macOS dev laptop)
produce a partial record marked "cluster_node_supported": false.
"""

import datetime as _dt
import glob
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys

SCHEMA_VERSION = 1

_warnings = []

# Pseudo block devices that are not storage for staged model weights.
VIRTUAL_DISKS = ("loop", "nbd", "ram", "zram", "dm-")


def _warn(msg):
    _warnings.append(msg)


def _run(cmd, timeout=10):
    """Return stripped stdout, or None if the command is missing or fails."""
    if shutil.which(cmd[0]) is None:
        return None
    try:
        p = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout,
            universal_newlines=True,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if p.returncode != 0:
        return None
    return p.stdout.strip()


def _read(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return None


def _int(s):
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def _first_line(s):
    return s.splitlines()[0].strip() if s else None


# ---- Linux collectors -------------------------------------------------------------------


def _lscpu():
    out = _run(["lscpu", "-J"])
    if not out:
        return {}
    flat = {}

    def walk(items):
        for it in items:
            flat[it.get("field", "").rstrip(":")] = it.get("data")
            walk(it.get("children", []))

    try:
        walk(json.loads(out).get("lscpu", []))
    except ValueError:
        return {}
    return flat


def _sysfs_cores():
    """Count distinct (package, core) pairs; works where lscpu omits socket counts."""
    cores = set()
    for d in glob.glob("/sys/devices/system/cpu/cpu[0-9]*/topology"):
        pkg, core = _read(d + "/physical_package_id"), _read(d + "/core_id")
        if pkg is None or core is None:
            return None
        cores.add((pkg, core))
    return len(cores) or None


def _cpu_linux():
    lc = _lscpu()
    flags = set()
    model = None
    for line in (_read("/proc/cpuinfo") or "").splitlines():
        k, _, v = line.partition(":")
        k = k.strip()
        if k == "flags" and not flags:
            flags = set(v.split())
        elif k == "model name" and model is None:
            model = v.strip()
    sockets = _int(lc.get("Socket(s)"))
    cores_per_socket = _int(lc.get("Core(s) per socket"))
    physical = sockets * cores_per_socket if sockets and cores_per_socket else _sysfs_cores()
    lc_model = lc.get("Model name")
    if lc_model in (None, "", "-"):
        lc_model = None
    if not lc:
        _warn("lscpu unavailable; socket/core topology unknown")
    interesting = ["avx2", "avx512f", "avx512_vnni", "avx512_bf16", "amx_tile", "amx_bf16",
                   "amx_int8", "fma", "f16c"]
    return {
        "model": lc_model or model,
        "vendor": lc.get("Vendor ID"),
        "architecture": platform.machine(),
        "sockets": sockets,
        "cores_per_socket": cores_per_socket,
        "threads_per_core": _int(lc.get("Thread(s) per core")),
        "physical_cores": physical,
        "logical_cpus": os.cpu_count(),
        "max_mhz": lc.get("CPU max MHz"),
        "l3_cache": lc.get("L3 cache"),
        "isa_flags": sorted(f for f in interesting if f in flags),
        "scaling_governor": _read("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
    }


def _meminfo():
    info = {}
    for line in (_read("/proc/meminfo") or "").splitlines():
        k, _, v = line.partition(":")
        parts = v.split()
        if parts:
            info[k] = _int(parts[0])
    kb = 1024
    return {
        "total_bytes": info["MemTotal"] * kb if info.get("MemTotal") else None,
        "available_bytes": info["MemAvailable"] * kb if info.get("MemAvailable") else None,
        "swap_total_bytes": info["SwapTotal"] * kb if "SwapTotal" in info else None,
        "hugepages_total": info.get("HugePages_Total"),
    }


def _numa():
    nodes = []
    for d in sorted(glob.glob("/sys/devices/system/node/node[0-9]*")):
        nodes.append({"id": _int(d.rsplit("node", 1)[1]), "cpulist": _read(d + "/cpulist")})
    return {"node_count": len(nodes) or None, "nodes": nodes}


def _disks():
    out = _run(["lsblk", "-J", "-b", "-d", "-o", "NAME,SIZE,ROTA,TYPE,MODEL,TRAN"])
    if not out:
        _warn("lsblk unavailable; disk inventory missing")
        return []
    try:
        devs = json.loads(out).get("blockdevices", [])
    except ValueError:
        return []
    disks = []
    for d in devs:
        name = d.get("name") or ""
        if d.get("type") != "disk" or not _int(d.get("size")) or name.startswith(VIRTUAL_DISKS):
            continue
        rota = d.get("rota")
        disks.append({
            "name": d.get("name"),
            "size_bytes": _int(d.get("size")),
            "rotational": rota in (True, "1", 1),
            "model": (d.get("model") or "").strip() or None,
            "transport": d.get("tran"),
        })
    return disks


def _nics():
    addrs = {}
    out = _run(["ip", "-j", "addr"])
    if out:
        try:
            for it in json.loads(out):
                addrs[it.get("ifname")] = [
                    "%s/%s" % (a["local"], a["prefixlen"])
                    for a in it.get("addr_info", []) if a.get("family") == "inet"
                ]
        except (ValueError, KeyError):
            pass
    nics = []
    for path in sorted(glob.glob("/sys/class/net/*")):
        name = os.path.basename(path)
        if name == "lo" or not os.path.isdir(path):  # skip e.g. bonding_masters
            continue
        physical = os.path.exists(path + "/device")
        speed = _int(_read(path + "/speed"))  # Mb/s; -1 or unreadable when link is down
        driver = None
        if physical and os.path.islink(path + "/device/driver"):
            driver = os.path.basename(os.readlink(path + "/device/driver"))
        nics.append({
            "name": name,
            "physical": physical,
            "driver": driver,
            "mac": _read(path + "/address"),
            "operstate": _read(path + "/operstate"),
            "speed_mbps": speed if speed and speed > 0 else None,
            "mtu": _int(_read(path + "/mtu")),
            "ipv4": addrs.get(name, []),
        })
    return nics


def _nfs_mounts():
    mounts = []
    for line in (_read("/proc/mounts") or "").splitlines():
        parts = line.split()
        if len(parts) >= 4 and parts[2] in ("nfs", "nfs4"):
            mounts.append({"source": parts[0], "target": parts[1], "fstype": parts[2],
                           "options": parts[3]})
    return mounts


def _time_sync():
    synced = _run(["timedatectl", "show", "-p", "NTPSynchronized", "--value"])
    service = None
    for svc in ("chrony", "chronyd", "systemd-timesyncd", "ntp", "ntpd"):
        if _run(["systemctl", "is-active", svc]) == "active":
            service = svc
            break
    if synced != "yes":
        _warn("clock not reported as NTP-synchronized (PRD F1 requires time sync)")
    return {"ntp_synchronized": {"yes": True, "no": False}.get(synced), "service": service}


def _os_linux():
    rel = {}
    for line in (_read("/etc/os-release") or "").splitlines():
        k, _, v = line.partition("=")
        rel[k] = v.strip('"')
    cgroup = "v2" if os.path.exists("/sys/fs/cgroup/cgroup.controllers") else (
        "v1" if os.path.isdir("/sys/fs/cgroup") else None)
    return {
        "system": "Linux",
        "distro": rel.get("ID"),
        "distro_version": rel.get("VERSION_ID"),
        "pretty_name": rel.get("PRETTY_NAME"),
        "kernel": platform.release(),
        "cgroup_version": cgroup,
    }


# ---- macOS fallback (dev machines only) ------------------------------------------------


def _sysctl(key):
    return _run(["sysctl", "-n", key])


def _collect_darwin():
    _warn("macOS host: not a supported cluster node; record is for tooling tests only")
    mem = _int(_sysctl("hw.memsize"))
    return {
        "os": {"system": "Darwin", "distro": "macos",
               "distro_version": platform.mac_ver()[0] or None,
               "pretty_name": "macOS " + (platform.mac_ver()[0] or ""),
               "kernel": platform.release(), "cgroup_version": None},
        "cpu": {"model": _sysctl("machdep.cpu.brand_string"), "vendor": None,
                "architecture": platform.machine(), "sockets": 1, "cores_per_socket": None,
                "threads_per_core": None, "physical_cores": _int(_sysctl("hw.physicalcpu")),
                "logical_cpus": os.cpu_count(), "max_mhz": None, "l3_cache": None,
                "isa_flags": [], "scaling_governor": None},
        "memory": {"total_bytes": mem, "available_bytes": None, "swap_total_bytes": None,
                   "hugepages_total": None},
        "numa": {"node_count": None, "nodes": []},
        "disks": [], "nics": [], "nfs_mounts": [],
        "time_sync": {"ntp_synchronized": None, "service": None},
    }


# ---- software versions ------------------------------------------------------------------


def _software():
    def ver(*cmds):
        for cmd in cmds:
            out = _run(cmd)
            if out:
                return _first_line(out)
        return None

    sw = {
        "python3": sys.version.split()[0],
        "docker_client": ver(["docker", "version", "--format", "{{.Client.Version}}"],
                             ["docker", "--version"]),
        "docker_server": ver(["docker", "version", "--format", "{{.Server.Version}}"]),
        "podman": ver(["podman", "--version"]),
        "apptainer": ver(["apptainer", "--version"], ["singularity", "--version"]),
        "slurm": ver(["sinfo", "--version"], ["slurmd", "-V"], ["srun", "--version"]),
        "mpirun": ver(["mpirun", "--version"]),
        "pmix": ver(["pmix_info", "--version"]),
        "simgrid": ver(["pkg-config", "--modversion", "simgrid"]),
        "nfs_utils": ver(["dpkg-query", "-W", "-f=${Version}", "nfs-common"]),
        "node_exporter": ver(["prometheus-node-exporter", "--version"],
                             ["node_exporter", "--version"]),
        "gcc": ver(["gcc", "--version"]),
    }
    if sw["docker_client"] and not sw["docker_server"]:
        _warn("docker daemon not reachable by this user; server version unknown")
    return sw


def collect():
    system = platform.system()
    if system == "Linux":
        rec = {
            "os": _os_linux(), "cpu": _cpu_linux(), "memory": _meminfo(), "numa": _numa(),
            "disks": _disks(), "nics": _nics(), "nfs_mounts": _nfs_mounts(),
            "time_sync": _time_sync(),
        }
        supported = True
    elif system == "Darwin":
        rec, supported = _collect_darwin(), False
    else:
        raise SystemExit("unsupported platform: %s" % system)

    out = {
        "schema_version": SCHEMA_VERSION,
        "collected_at": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hostname": socket.gethostname().split(".")[0],
        "fqdn": socket.getfqdn(),
        "cluster_node_supported": supported,
    }
    out.update(rec)
    out["software"] = _software()
    out["warnings"] = list(_warnings)
    return out


def main():
    json.dump(collect(), sys.stdout, indent=2, sort_keys=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
