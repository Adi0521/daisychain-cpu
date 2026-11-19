"""Load and apply the JSON schemas in /schemas."""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path

from jsonschema import Draft202012Validator

SCHEMA_NAMES = ("inventory", "experiment_manifest", "request_record", "run_summary")


def _schema_dir() -> Path:
    installed = Path(__file__).parent / "_schemas"
    if installed.is_dir():
        return installed
    return Path(__file__).resolve().parents[2] / "schemas"


@cache
def validator(name: str) -> Draft202012Validator:
    if name not in SCHEMA_NAMES:
        raise KeyError(f"unknown schema {name!r}; choose from {', '.join(SCHEMA_NAMES)}")
    schema = json.loads((_schema_dir() / f"{name}.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)


def errors(name: str, instance) -> list[str]:
    """Human-readable validation errors (empty list means valid)."""
    out = []
    for e in sorted(validator(name).iter_errors(instance), key=lambda e: list(e.path)):
        where = "/".join(str(p) for p in e.path) or "<root>"
        out.append(f"{where}: {e.message}")
    return out


def load_records(path: Path) -> list:
    """A .jsonl file yields one record per line; anything else is a single JSON document."""
    text = path.read_text()
    if path.suffix == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    return [json.loads(text)]
