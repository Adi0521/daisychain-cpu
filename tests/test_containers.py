"""Container launch helpers. Docker is not required.

The live check is `daisychain emu up && daisychain emu container-verify`.
"""

from pathlib import Path

from daisychain import containers, emu, slurm
from daisychain.topology import Topology

ROOT = Path(__file__).resolve().parents[1]


def test_launcher_targets_local_image_store():
    assert containers.launcher() == "apptainer exec /var/lib/daisychain/images/probe "
    assert containers.image_path("llm") == "/var/lib/daisychain/images/llm"


def test_probe_image_differs_from_node_os():
    # The identity check relies on the probe being Debian while nodes are Ubuntu.
    probe = (ROOT / "emu/probe/Dockerfile").read_text()
    node = (ROOT / "emu/node.Dockerfile").read_text()
    assert "bookworm" in probe and "FROM ubuntu:" in node


def test_node_image_installs_non_setuid_apptainer():
    node = (ROOT / "emu/node.Dockerfile").read_text()
    assert "install -y --no-install-recommends apptainer" in node
    assert "apptainer-suid" not in node


def test_user_uid_matches_image():
    node = (ROOT / "emu/node.Dockerfile").read_text()
    assert f"useradd -m -u {slurm.USER_UID}" in node


def test_launcher_prefix_composes_with_check_commands():
    # Checks build "srun … {launcher}bash -c '…'"; the prefix must end with a space.
    assert containers.launcher("x").endswith(" ")
    plan = emu.make_plan(Topology.load(ROOT / "config/topology/chain.yaml"), n_workers=2)
    assert plan.hosts == ["C0", "W1", "W2"]
