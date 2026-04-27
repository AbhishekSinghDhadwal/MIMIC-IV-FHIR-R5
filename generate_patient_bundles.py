#!/usr/bin/env python3
"""Generate atomic patient-centric R5 transaction bundles from MIMIC NDJSON."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import json
import os
import socket
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib
from collections import Counter, deque
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from tqdm import tqdm


SCRIPT_VERSION = "2026.04.18.1"
FHIR_ACCEPT = "application/fhir+json"
TEST_IDENTIFIER_SYSTEM = "https://example.org/codex/mimic/test-id"
SQLITE_CACHE_MB = 32
VACUUM_EVERY_PURGED_PATIENTS = 25

FILE_ORDER: Sequence[Dict[str, Any]] = (
    {"name": "MimicPatient.ndjson", "kind": "patient", "resourceType": "Patient"},
    {"name": "MimicOrganization.ndjson", "kind": "shared", "resourceType": "Organization"},
    {"name": "MimicLocation.ndjson", "kind": "shared", "resourceType": "Location"},
    {"name": "MimicMedication.ndjson", "kind": "shared", "resourceType": "Medication"},
    {"name": "MimicMedicationMix.ndjson", "kind": "shared", "resourceType": "Medication"},
    {"name": "MimicEncounter.ndjson", "kind": "patient_owned", "resourceType": "Encounter"},
    {"name": "MimicEncounterED.ndjson", "kind": "patient_owned", "resourceType": "Encounter"},
    {"name": "MimicEncounterICU.ndjson", "kind": "patient_owned", "resourceType": "Encounter"},
    {"name": "MimicCondition.ndjson", "kind": "patient_owned", "resourceType": "Condition"},
    {"name": "MimicConditionED.ndjson", "kind": "patient_owned", "resourceType": "Condition"},
    {"name": "MimicMedicationAdministration.ndjson", "kind": "patient_owned", "resourceType": "MedicationAdministration"},
    {"name": "MimicMedicationAdministrationICU.ndjson", "kind": "patient_owned", "resourceType": "MedicationAdministration"},
    {"name": "MimicMedicationDispense.ndjson", "kind": "patient_owned", "resourceType": "MedicationDispense"},
    {"name": "MimicMedicationDispenseED.ndjson", "kind": "patient_owned", "resourceType": "MedicationDispense"},
    {"name": "MimicMedicationRequest.ndjson", "kind": "patient_owned", "resourceType": "MedicationRequest"},
    {"name": "MimicMedicationStatementED.ndjson", "kind": "patient_owned", "resourceType": "MedicationStatement"},
    {"name": "MimicObservationChartevents.ndjson", "kind": "patient_owned", "resourceType": "Observation"},
    {"name": "MimicObservationDatetimeevents.ndjson", "kind": "patient_owned", "resourceType": "Observation"},
    {"name": "MimicObservationED.ndjson", "kind": "patient_owned", "resourceType": "Observation"},
    {"name": "MimicObservationLabevents.ndjson", "kind": "patient_owned", "resourceType": "Observation"},
    {"name": "MimicObservationMicroOrg.ndjson", "kind": "patient_owned", "resourceType": "Observation"},
    {"name": "MimicObservationMicroSusc.ndjson", "kind": "patient_owned", "resourceType": "Observation"},
    {"name": "MimicObservationMicroTest.ndjson", "kind": "patient_owned", "resourceType": "Observation"},
    {"name": "MimicObservationOutputevents.ndjson", "kind": "patient_owned", "resourceType": "Observation"},
    {"name": "MimicObservationVitalSignsED.ndjson", "kind": "patient_owned", "resourceType": "Observation"},
    {"name": "MimicProcedure.ndjson", "kind": "patient_owned", "resourceType": "Procedure"},
    {"name": "MimicProcedureED.ndjson", "kind": "patient_owned", "resourceType": "Procedure"},
    {"name": "MimicProcedureICU.ndjson", "kind": "patient_owned", "resourceType": "Procedure"},
    {"name": "MimicSpecimen.ndjson", "kind": "patient_owned", "resourceType": "Specimen"},
    {"name": "MimicSpecimenLab.ndjson", "kind": "patient_owned", "resourceType": "Specimen"},
)
PATIENT_FILE_SPEC = FILE_ORDER[0]

SHARED_RESOURCE_TYPES = {"Organization", "Location", "Medication"}
PREFERRED_SAMPLE_PATIENTS = [
    "00000027-c5e0-554f-8e85-b097c3b177d4",
    "000000ba-735e-5858-a92d-b856d73dd69a",
    "000048d5-5ac9-57bc-9316-3c582c37b884",
    "0000ad47-7103-5f54-970d-dafc42fd12f9",
    "0000c20f-4079-5da2-a3ed-0a5118ec6184",
    "00011d50-5301-59a9-a134-88499850696a",
]
REFERENCE_OWNER_KEYS = ("subject", "patient", "beneficiary", "individual")
FHIR_ID_IDENTIFIER_TYPES = {
    "Patient",
    "Encounter",
    "Condition",
    "MedicationAdministration",
    "MedicationDispense",
    "MedicationRequest",
    "MedicationStatement",
    "Observation",
    "Organization",
    "Location",
    "Medication",
    "Procedure",
    "Specimen",
}
MEDICATION_STATEMENT_STATUS_MAP = {
    "active": "recorded",
    "completed": "recorded",
    "intended": "draft",
    "stopped": "recorded",
    "on-hold": "recorded",
    "unknown": "recorded",
    "not-taken": "recorded",
}
ENCOUNTER_STATUS_MAP = {
    "finished": "completed",
}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def gzip_json_blob(resource: Dict[str, Any]) -> bytes:
    return zlib.compress(json_dumps(resource).encode("utf-8"), level=6)


def ungzip_json_blob(blob: bytes) -> Dict[str, Any]:
    return json.loads(zlib.decompress(blob).decode("utf-8"))


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write_text(path, json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def path_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            total += child.stat().st_size
    return total


def append_jsonl(path: Path, value: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(value, ensure_ascii=True) + "\n")


def record_timing(stats: "Stats", phase: str, elapsed_seconds: float) -> None:
    stats.data["timing_seconds"][phase] = round(elapsed_seconds, 3)
    print(f"[timing] {phase}={elapsed_seconds:.3f}s", flush=True)


def relative_ref(resource_type: str, resource_id: str) -> str:
    return f"{resource_type}/{resource_id}"


def parse_relative_reference(reference: str) -> Optional[Tuple[str, str]]:
    if not reference or reference.startswith("#") or reference.startswith("urn:uuid:"):
        return None
    if reference.startswith("http://") or reference.startswith("https://"):
        parsed = urllib.parse.urlparse(reference)
        path = parsed.path.strip("/")
    else:
        path = reference.strip("/")
    parts = path.split("/")
    if len(parts) < 2:
        return None
    if parts[-2] == "_history":
        parts = parts[:-2]
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return None


def collect_reference_strings(node: Any) -> Iterator[str]:
    if isinstance(node, dict):
        ref = node.get("reference")
        if isinstance(ref, str):
            yield ref
        for value in node.values():
            yield from collect_reference_strings(value)
    elif isinstance(node, list):
        for item in node:
            yield from collect_reference_strings(item)


def rewrite_reference_strings(node: Any, mapper) -> None:
    if isinstance(node, dict):
        ref = node.get("reference")
        if isinstance(ref, str):
            mapped = mapper(ref)
            if mapped is not None:
                node["reference"] = mapped
        for value in node.values():
            rewrite_reference_strings(value, mapper)
    elif isinstance(node, list):
        for item in node:
            rewrite_reference_strings(item, mapper)


def first_identifier_value(resource: Dict[str, Any]) -> str:
    for ident in resource.get("identifier", []):
        value = ident.get("value")
        if value:
            return str(value)
    return ""


def append_identifier(resource: Dict[str, Any], system: str, value: str) -> None:
    resource.setdefault("identifier", [])
    resource["identifier"].append({"system": system, "value": value})


def hash_suffix(text: str, length: int = 20) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:length]


def stable_fallback_id(resource_type: str, identity: str) -> str:
    return f"mimic-{resource_type.lower()}-{hash_suffix(identity, 24)}"


def uuid_for_key(key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def sanitize_server_base(server_url: str) -> str:
    return server_url.rstrip("/")


def classify_import_failure(status_code: int, body_text: str) -> str:
    lowered = body_text.lower()
    if "timed out" in lowered or "timeout" in lowered or "socket timeout" in lowered:
        return "transport/timeout"
    if "temporary failure" in lowered or "name or service not known" in lowered or "connection reset" in lowered:
        return "transport/network"
    if "client-assigned" in lowered or "updatecreate" in lowered or "does not exist" in lowered:
        return "duplicate/identity issue"
    if "reference" in lowered or "unable to resolve" in lowered:
        return "reference resolution"
    if "unknown element" in lowered or "additional property" in lowered or "unrecognized field" in lowered:
        return "unsupported field/resource"
    if "profile" in lowered or "validation" in lowered:
        return "profile/validation issue"
    if "r4" in lowered or "r5" in lowered or "fhir version" in lowered:
        return "version mismatch"
    if status_code in (400, 404, 405, 409, 412, 422):
        return "transaction packaging"
    return "production-safety constraint"


def gz_name(spec: Dict[str, Any]) -> str:
    return f"{spec['name']}.gz"


def collect_source_file_metadata(paths: Sequence[Path]) -> Dict[str, Dict[str, Any]]:
    metadata: Dict[str, Dict[str, Any]] = {}
    for path in paths:
        stat = path.stat()
        metadata[path.name] = {
            "path": str(path.resolve()),
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
    return metadata


def candidate_input_roots(input_path: Path) -> List[Path]:
    roots = [input_path]
    roots.extend(input_path.parents)
    if input_path.is_dir():
        for depth_1 in input_path.iterdir():
            if depth_1.is_dir():
                roots.append(depth_1)
                for depth_2 in depth_1.iterdir():
                    if depth_2.is_dir():
                        roots.append(depth_2)
    seen: set[Path] = set()
    unique: List[Path] = []
    for root in roots:
        if root not in seen:
            seen.add(root)
            unique.append(root)
    return unique


def detect_dataset_paths(input_path: Path) -> Dict[str, Optional[Path]]:
    resolved = input_path.resolve()
    repo_root: Optional[Path] = None
    ndjson_dir: Optional[Path] = None
    for candidate in candidate_input_roots(resolved):
        if candidate.name == "fhir" and (
            (candidate / "MimicPatient.ndjson").exists() or (candidate / "MimicPatient.ndjson.gz").exists()
        ):
            ndjson_dir = candidate
            repo_root = candidate.parent
            break
        fhir_dir = candidate / "fhir"
        if fhir_dir.exists() and (
            (fhir_dir / "MimicPatient.ndjson").exists() or (fhir_dir / "MimicPatient.ndjson.gz").exists()
        ):
            ndjson_dir = fhir_dir
            repo_root = candidate
            break
    if repo_root is None or ndjson_dir is None:
        raise FileNotFoundError(
            f"Could not locate a MIMIC FHIR 'fhir/' folder under {input_path}"
        )

    ig_package_dir: Optional[Path] = None
    search_roots = [repo_root, repo_root.parent, repo_root.parent.parent if repo_root.parent != repo_root else repo_root]
    for root in search_roots:
        if not root or not root.exists():
            continue
        direct = root / "package" / "ImplementationGuide-kindlab.fhir.mimic.json"
        if direct.exists():
            ig_package_dir = direct.parent
            break
        for child in root.iterdir():
            if child.is_dir():
                candidate = child / "package" / "ImplementationGuide-kindlab.fhir.mimic.json"
                if candidate.exists():
                    ig_package_dir = candidate.parent
                    break
        if ig_package_dir:
            break
    return {"repo_root": repo_root, "ndjson_dir": ndjson_dir, "ig_package_dir": ig_package_dir}


def prepare_input_directory(
    source_dir: Path,
    output_dir: Path,
    use_gz: bool,
    force: bool,
) -> Tuple[Path, Dict[str, Any]]:
    if use_gz:
        source_paths = [source_dir / gz_name(spec) for spec in FILE_ORDER]
        missing = [path.name for path in source_paths if not path.exists()]
        if missing:
            raise FileNotFoundError(
                "Compressed NDJSON input files were requested with --gz, but these files are missing under "
                f"{source_dir}: {', '.join(missing)}"
            )

        staged_dir = output_dir / "logs" / "extracted_ndjson"
        ensure_dir(staged_dir)
        events: List[Dict[str, Any]] = []
        failure_log = output_dir / "logs" / "skipped_resources.ndjson"
        max_staged_bytes = 0

        for spec, source_path in zip(FILE_ORDER, source_paths):
            dest_path = staged_dir / spec["name"]
            should_extract = (
                force
                or not dest_path.exists()
                or source_path.stat().st_mtime_ns > dest_path.stat().st_mtime_ns
            )
            if not should_extract:
                max_staged_bytes = max(max_staged_bytes, dest_path.stat().st_size)
                events.append(
                    {
                        "event": "gzip_reused",
                        "source": str(source_path.resolve()),
                        "destination": str(dest_path.resolve()),
                    }
                )
                continue

            tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
            try:
                with gzip.open(source_path, "rb") as src, tmp_path.open("wb") as dst:
                    while True:
                        chunk = src.read(1024 * 1024)
                        if not chunk:
                            break
                        dst.write(chunk)
                os.replace(tmp_path, dest_path)
                source_stat = source_path.stat()
                os.utime(dest_path, ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns))
                max_staged_bytes = max(max_staged_bytes, dest_path.stat().st_size)
                events.append(
                    {
                        "event": "gzip_extracted",
                        "source": str(source_path.resolve()),
                        "destination": str(dest_path.resolve()),
                    }
                )
            except Exception as exc:
                if tmp_path.exists():
                    tmp_path.unlink()
                append_jsonl(
                    failure_log,
                    {
                        "event": "gzip_extract_failed",
                        "source": str(source_path.resolve()),
                        "destination": str(dest_path.resolve()),
                        "reason": str(exc),
                    },
                )
                raise RuntimeError(f"Failed to extract {source_path.name}: {exc}") from exc
        return staged_dir, {
            "mode": "gz",
            "source_dir": str(source_dir.resolve()),
            "staged_dir": str(staged_dir.resolve()),
            "source_files": collect_source_file_metadata(source_paths),
            "max_staged_file_bytes": max_staged_bytes,
            "events": events,
        }

    source_paths = [source_dir / spec["name"] for spec in FILE_ORDER]
    missing = [path.name for path in source_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Extracted NDJSON files are required by default, but these files are missing under "
            f"{source_dir}: {', '.join(missing)}. Rerun with --gz to stage them from the compressed corpus."
        )
    return source_dir, {
        "mode": "ndjson",
        "source_dir": str(source_dir.resolve()),
        "staged_dir": None,
        "source_files": collect_source_file_metadata(source_paths),
        "events": [],
    }


def materialize_scan_input(ctx: "Context", spec: Dict[str, Any]) -> Path:
    if ctx.input_prep.get("mode") != "gz":
        return ctx.ndjson_dir / spec["name"]

    staged_dir = Path(ctx.input_prep["staged_dir"])
    dest_path = staged_dir / spec["name"]
    return dest_path


def declared_scan_input_path(ctx: "Context", spec: Dict[str, Any]) -> Path:
    if ctx.input_prep.get("mode") == "gz":
        return Path(ctx.input_prep["source_dir"]) / gz_name(spec)
    return ctx.ndjson_dir / spec["name"]


def cleanup_staged_scan_input(ctx: "Context", spec: Dict[str, Any], path: Path) -> None:
    return


class ProgressPrinter:
    def __init__(self, label: str, interval_seconds: float = 8.0) -> None:
        self.label = label
        self.interval_seconds = interval_seconds
        self.last = time.time()

    def maybe(self, line_number: int, extra: str = "") -> None:
        now = time.time()
        if now - self.last >= self.interval_seconds:
            suffix = f" {extra}" if extra else ""
            print(f"[scan] {self.label}: {line_number:,} lines{suffix}", flush=True)
            self.last = now


class CsvWriter:
    def __init__(self, path: Path, fieldnames: Sequence[str]) -> None:
        self.path = path
        ensure_dir(path.parent)
        self.handle = path.open("w", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(self.handle, fieldnames=fieldnames)
        self.writer.writeheader()

    def row(self, values: Dict[str, Any]) -> None:
        self.writer.writerow(values)

    def close(self) -> None:
        self.handle.close()


class JsonlWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        ensure_dir(path.parent)
        self.handle = path.open("w", encoding="utf-8", newline="\n")

    def row(self, value: Dict[str, Any]) -> None:
        self.handle.write(json.dumps(value, ensure_ascii=True) + "\n")

    def close(self) -> None:
        self.handle.close()


class Stats:
    def __init__(self) -> None:
        self.data: Dict[str, Any] = {
            "script_version": SCRIPT_VERSION,
            "started_at": utc_now(),
            "resources_processed_by_type": Counter(),
            "resources_processed_by_file": Counter(),
            "resources_emitted_by_type": Counter(),
            "resources_omitted_by_type": Counter(),
            "unresolved_reference_count": 0,
            "malformed_line_count": 0,
            "bundles_emitted": 0,
            "bundles_blocked": 0,
            "custom_patches_applied": Counter(),
            "gzip_events_by_type": Counter(),
            "patients_selected": 0,
            "selection_offset": 0,
            "selection_num_patients": None,
            "patient_payload_rows_purged": 0,
            "patient_records_purged": 0,
            "encounter_mappings_purged": 0,
            "sqlite_bytes_after_scan": 0,
            "sqlite_bytes_after_build": 0,
            "input_source_bytes": 0,
            "bundle_output_bytes": 0,
            "gzip_max_staged_file_bytes": 0,
            "gzip_staged_bytes_total": 0,
            "timing_seconds": {},
            "notes": [],
        }

    def note(self, message: str) -> None:
        self.data["notes"].append(message)

    def as_json(self) -> Dict[str, Any]:
        out = deepcopy(self.data)
        for key in (
            "resources_processed_by_type",
            "resources_processed_by_file",
            "resources_emitted_by_type",
            "resources_omitted_by_type",
            "custom_patches_applied",
            "gzip_events_by_type",
        ):
            out[key] = dict(sorted(out[key].items()))
        out["finished_at"] = utc_now()
        return out


class SelectionFilter:
    def __init__(self, explicit_ids: Sequence[str], num_patients: Optional[int], offset: int) -> None:
        self.explicit_ids = [value.strip() for value in explicit_ids if value.strip()]
        self.explicit_set = set(self.explicit_ids)
        self.num_patients = num_patients
        self.offset = max(0, offset)
        self.selected_order: List[str] = []
        self.selected_set: set[str] = set()
        self.total_patient_records = 0

    @property
    def is_filtered(self) -> bool:
        return bool(self.explicit_ids) or self.num_patients is not None or self.offset > 0

    def plan_patient_record(self, patient_id: str) -> bool:
        ordinal = self.total_patient_records
        self.total_patient_records += 1
        if self.explicit_set:
            if patient_id in self.explicit_set:
                if patient_id not in self.selected_set:
                    self.selected_set.add(patient_id)
                    self.selected_order.append(patient_id)
                return True
            return False
        if ordinal < self.offset:
            return False
        if self.num_patients is None:
            self.selected_set.add(patient_id)
            self.selected_order.append(patient_id)
            return True
        if patient_id in self.selected_set:
            return True
        if len(self.selected_order) < self.num_patients:
            self.selected_order.append(patient_id)
            self.selected_set.add(patient_id)
            return True
        return False

    def accept_owner(self, patient_id: Optional[str]) -> bool:
        if not patient_id:
            return False
        if not self.is_filtered:
            return True
        return patient_id in self.selected_set or patient_id in self.explicit_set

    def ordered_ids(self) -> List[str]:
        if self.explicit_ids:
            return [patient_id for patient_id in self.explicit_ids if patient_id in self.selected_set]
        return list(self.selected_order)


class WorkDb:
    def __init__(self, path: Path) -> None:
        self.path = path
        ensure_dir(path.parent)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=DELETE")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=FILE")
        self.conn.execute(f"PRAGMA cache_size=-{SQLITE_CACHE_MB * 1024}")
        self.conn.execute("PRAGMA mmap_size=0")
        self.conn.execute("PRAGMA auto_vacuum=INCREMENTAL")

    def close(self) -> None:
        self.conn.close()

    def reset(self) -> None:
        self.conn.executescript(
            """
            DROP TABLE IF EXISTS meta;
            DROP TABLE IF EXISTS patient_index;
            DROP TABLE IF EXISTS patients;
            DROP TABLE IF EXISTS patient_resources;
            DROP TABLE IF EXISTS shared_resources;
            DROP TABLE IF EXISTS encounter_patient;
            DROP TABLE IF EXISTS manifest_seed;
            DROP TABLE IF EXISTS scan_state;
            """
        )
        self.conn.commit()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS patient_index (
              patient_id TEXT PRIMARY KEY,
              patient_identifier TEXT,
              patient_ordinal INTEGER NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS patients (
              patient_id TEXT PRIMARY KEY,
              identifier_value TEXT,
              raw_blob BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS patient_resources (
              patient_id TEXT NOT NULL,
              resource_type TEXT NOT NULL,
              source_id TEXT NOT NULL,
              raw_blob BLOB NOT NULL,
              source_file TEXT NOT NULL,
              PRIMARY KEY (patient_id, resource_type, source_id)
            );
            CREATE TABLE IF NOT EXISTS shared_resources (
              resource_type TEXT NOT NULL,
              source_id TEXT NOT NULL,
              raw_blob BLOB NOT NULL,
              source_file TEXT NOT NULL,
              PRIMARY KEY (resource_type, source_id)
            );
            CREATE TABLE IF NOT EXISTS encounter_patient (
              encounter_id TEXT PRIMARY KEY,
              patient_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS manifest_seed (
              patient_id TEXT PRIMARY KEY,
              patient_identifier TEXT,
              patient_ordinal INTEGER NOT NULL,
              owned_count INTEGER NOT NULL DEFAULT 0,
              emitted_count INTEGER NOT NULL DEFAULT 0,
              shared_count INTEGER NOT NULL DEFAULT 0,
              unresolved_count INTEGER NOT NULL DEFAULT 0,
              status TEXT NOT NULL DEFAULT 'pending',
              bundle_path TEXT,
              note TEXT
            );
            CREATE TABLE IF NOT EXISTS scan_state (
              source_file TEXT PRIMARY KEY,
              completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_patient_index_ordinal ON patient_index(patient_ordinal);
            CREATE INDEX IF NOT EXISTS idx_patient_resources_patient ON patient_resources(patient_id);
            CREATE INDEX IF NOT EXISTS idx_encounter_patient_patient ON encounter_patient(patient_id);
            CREATE INDEX IF NOT EXISTS idx_shared_resources_type_id ON shared_resources(resource_type, source_id);
            """
        )
        self.conn.commit()

    def ensure_meta(self, expected: Dict[str, str], resume: bool) -> bool:
        self.init_schema()
        rows = {row["key"]: row["value"] for row in self.conn.execute("SELECT key, value FROM meta")}
        if not rows:
            for key, value in expected.items():
                self.conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", (key, value))
            self.conn.commit()
            return True
        if rows != expected:
            if resume:
                raise RuntimeError(
                    "Existing staging DB was created with different inputs or options. "
                    "Use --force to rebuild it for this run."
                )
            self.reset()
            self.init_schema()
            for key, value in expected.items():
                self.conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", (key, value))
            self.conn.commit()
            return True
        return False

    def is_scanned(self, source_file: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM scan_state WHERE source_file = ?",
            (source_file,),
        ).fetchone()
        return row is not None

    def mark_scanned(self, source_file: str) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO scan_state(source_file, completed_at) VALUES (?, ?)",
            (source_file, utc_now()),
        )
        self.conn.commit()

    def upsert_patient(self, resource: Dict[str, Any], identifier_value: str) -> None:
        raise RuntimeError("upsert_patient now requires patient_ordinal")

    def upsert_patient_index(self, patient_id: str, identifier_value: str, patient_ordinal: int) -> None:
        self.conn.execute(
            """
            INSERT INTO patient_index(patient_id, patient_identifier, patient_ordinal)
            VALUES (?, ?, ?)
            ON CONFLICT(patient_id) DO UPDATE SET
              patient_identifier = excluded.patient_identifier,
              patient_ordinal = excluded.patient_ordinal
            """,
            (patient_id, identifier_value, patient_ordinal),
        )

    def upsert_selected_patient(
        self,
        resource: Dict[str, Any],
        identifier_value: str,
        patient_ordinal: int,
    ) -> None:
        patient_id = resource["id"]
        self.conn.execute(
            "INSERT OR REPLACE INTO patients(patient_id, identifier_value, raw_blob) VALUES (?, ?, ?)",
            (patient_id, identifier_value, gzip_json_blob(resource)),
        )
        self.conn.execute(
            """
            INSERT INTO manifest_seed(patient_id, patient_identifier, patient_ordinal, status)
            VALUES (?, ?, ?, 'pending')
            ON CONFLICT(patient_id) DO UPDATE SET
              patient_identifier = excluded.patient_identifier,
              patient_ordinal = excluded.patient_ordinal
            """,
            (patient_id, identifier_value, patient_ordinal),
        )

    def upsert_patient_resource(
        self,
        patient_id: str,
        resource_type: str,
        source_id: str,
        source_file: str,
        resource: Dict[str, Any],
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO patient_resources(patient_id, resource_type, source_id, raw_blob, source_file)
            VALUES (?, ?, ?, ?, ?)
            """,
            (patient_id, resource_type, source_id, gzip_json_blob(resource), source_file),
        )
        self.conn.execute(
            "UPDATE manifest_seed SET owned_count = owned_count + 1 WHERE patient_id = ?",
            (patient_id,),
        )

    def upsert_shared_resource(
        self,
        resource_type: str,
        source_id: str,
        source_file: str,
        resource: Dict[str, Any],
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO shared_resources(resource_type, source_id, raw_blob, source_file)
            VALUES (?, ?, ?, ?)
            """,
            (resource_type, source_id, gzip_json_blob(resource), source_file),
        )

    def map_encounter_to_patient(self, encounter_id: str, patient_id: str) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO encounter_patient(encounter_id, patient_id) VALUES (?, ?)",
            (encounter_id, patient_id),
        )

    def lookup_encounter_patient(self, encounter_id: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT patient_id FROM encounter_patient WHERE encounter_id = ?",
            (encounter_id,),
        ).fetchone()
        return None if row is None else str(row["patient_id"])

    def get_patient(self, patient_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT raw_blob FROM patients WHERE patient_id = ?",
            (patient_id,),
        ).fetchone()
        return None if row is None else ungzip_json_blob(row["raw_blob"])

    def get_resource_for_patient(
        self,
        patient_id: str,
        resource_type: str,
        source_id: str,
    ) -> Optional[Dict[str, Any]]:
        if resource_type == "Patient":
            return self.get_patient(source_id) if source_id == patient_id else None
        if resource_type in SHARED_RESOURCE_TYPES:
            row = self.conn.execute(
                "SELECT raw_blob FROM shared_resources WHERE resource_type = ? AND source_id = ?",
                (resource_type, source_id),
            ).fetchone()
            return None if row is None else ungzip_json_blob(row["raw_blob"])
        row = self.conn.execute(
            """
            SELECT raw_blob FROM patient_resources
            WHERE patient_id = ? AND resource_type = ? AND source_id = ?
            """,
            (patient_id, resource_type, source_id),
        ).fetchone()
        return None if row is None else ungzip_json_blob(row["raw_blob"])

    def iter_patient_owned_resources(self, patient_id: str) -> Iterator[Dict[str, Any]]:
        cursor = self.conn.execute(
            "SELECT raw_blob FROM patient_resources WHERE patient_id = ? ORDER BY resource_type, source_id",
            (patient_id,),
        )
        for row in cursor:
            yield ungzip_json_blob(row["raw_blob"])

    def iter_manifest_rows(self) -> Iterator[sqlite3.Row]:
        cursor = self.conn.execute(
            "SELECT * FROM manifest_seed ORDER BY patient_ordinal, patient_id"
        )
        for row in cursor:
            yield row

    def get_selected_patient_ids(self) -> List[str]:
        cursor = self.conn.execute(
            "SELECT patient_id FROM manifest_seed ORDER BY patient_ordinal, patient_id"
        )
        return [str(row["patient_id"]) for row in cursor]

    def count_patient_index_rows(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS count_value FROM patient_index").fetchone()
        return 0 if row is None else int(row["count_value"])

    def purge_patient_payload(self, patient_id: str) -> Dict[str, int]:
        patient_row = self.conn.execute(
            "DELETE FROM patients WHERE patient_id = ?",
            (patient_id,),
        )
        resource_row = self.conn.execute(
            "DELETE FROM patient_resources WHERE patient_id = ?",
            (patient_id,),
        )
        encounter_row = self.conn.execute(
            "DELETE FROM encounter_patient WHERE patient_id = ?",
            (patient_id,),
        )
        return {
            "patient_records": int(patient_row.rowcount or 0),
            "patient_resources": int(resource_row.rowcount or 0),
            "encounter_mappings": int(encounter_row.rowcount or 0),
        }

    def incremental_vacuum(self, pages: int = 2048) -> None:
        self.conn.execute(f"PRAGMA incremental_vacuum({pages})")

    def file_size_bytes(self) -> int:
        total = 0
        for suffix in ("", "-wal", "-shm", "-journal"):
            path = Path(str(self.path) + suffix)
            if path.exists():
                total += path.stat().st_size
        return total

    def update_manifest(
        self,
        patient_id: str,
        *,
        emitted_count: Optional[int] = None,
        shared_count: Optional[int] = None,
        unresolved_count: Optional[int] = None,
        status: Optional[str] = None,
        bundle_path: Optional[str] = None,
        note: Optional[str] = None,
    ) -> None:
        clauses = []
        values: List[Any] = []
        if emitted_count is not None:
            clauses.append("emitted_count = ?")
            values.append(emitted_count)
        if shared_count is not None:
            clauses.append("shared_count = ?")
            values.append(shared_count)
        if unresolved_count is not None:
            clauses.append("unresolved_count = ?")
            values.append(unresolved_count)
        if status is not None:
            clauses.append("status = ?")
            values.append(status)
        if bundle_path is not None:
            clauses.append("bundle_path = ?")
            values.append(bundle_path)
        if note is not None:
            clauses.append("note = ?")
            values.append(note)
        if not clauses:
            return
        values.append(patient_id)
        sql = "UPDATE manifest_seed SET " + ", ".join(clauses) + " WHERE patient_id = ?"
        self.conn.execute(sql, values)

    def commit(self) -> None:
        self.conn.commit()


class Context:
    def __init__(
        self,
        args: argparse.Namespace,
        repo_root: Path,
        ndjson_dir: Path,
        ig_package_dir: Optional[Path],
        output_dir: Path,
        db: WorkDb,
        input_prep: Dict[str, Any],
    ) -> None:
        self.args = args
        self.repo_root = repo_root
        self.ndjson_dir = ndjson_dir
        self.ig_package_dir = ig_package_dir
        self.output_dir = output_dir
        self.input_prep = input_prep
        self.bundles_dir = output_dir / "bundles"
        self.logs_dir = output_dir / "logs"
        self.db = db
        self.stats = Stats()
        self.purged_patients_since_vacuum = 0
        ensure_dir(self.bundles_dir)
        ensure_dir(self.logs_dir)
        self.unresolved_writer = CsvWriter(
            self.logs_dir / "unresolved_refs.csv",
            [
                "patient_id",
                "source_resource_type",
                "source_resource_id",
                "reference",
                "phase",
                "note",
            ],
        )
        self.conversion_writer = CsvWriter(
            self.logs_dir / "conversion_warnings.csv",
            ["patient_id", "resource_type", "resource_id", "warning_type", "details"],
        )
        self.skipped_writer = JsonlWriter(self.logs_dir / "skipped_resources.ndjson")
        self.import_results: Dict[str, Any] = {
            "discovery": {},
            "preflight_validation": {},
            "namespace": None,
            "attempts": [],
            "safety_decision": "",
        }

    def close(self) -> None:
        self.unresolved_writer.close()
        self.conversion_writer.close()
        self.skipped_writer.close()


def record_input_preparation(ctx: Context) -> None:
    mode = ctx.input_prep.get("mode", "unknown")
    ctx.stats.note(f"Input mode: {mode}")
    ctx.stats.data["input_source_bytes"] = sum(
        int(item.get("size", 0))
        for item in (ctx.input_prep.get("source_files") or {}).values()
    )
    if ctx.input_prep.get("staged_dir"):
        ctx.stats.note(f"Staged NDJSON directory: {ctx.input_prep['staged_dir']}")
        ctx.stats.data["gzip_staged_bytes_total"] = path_size_bytes(Path(ctx.input_prep["staged_dir"]))
    if mode == "gz":
        ctx.stats.data["gzip_max_staged_file_bytes"] = int(ctx.input_prep.get("max_staged_file_bytes", 0) or 0)
        ctx.stats.note(
            "Gzip mode expands the full compressed corpus into output/logs/extracted_ndjson "
            "before scanning and keeps those staged files for the run."
        )
    for event in ctx.input_prep.get("events", []):
        event_name = str(event.get("event", "unknown"))
        ctx.stats.data["gzip_events_by_type"][event_name] += 1
        ctx.skipped_writer.row(event)


def discover_server(server_url: str, timeout_seconds: int) -> Dict[str, Any]:
    url = sanitize_server_base(server_url) + "/metadata"
    request = urllib.request.Request(url, headers={"Accept": FHIR_ACCEPT})
    response = perform_fhir_post(request, timeout_seconds)
    try:
        payload = json.loads(response["body"])
    except json.JSONDecodeError:
        payload = {}
    rest = payload.get("rest", []) if isinstance(payload, dict) else []
    rest0 = rest[0] if rest else {}
    resource_map = {
        resource.get("type"): resource
        for resource in rest0.get("resource", [])
        if isinstance(resource, dict) and resource.get("type")
    }
    return {
        "url": url,
        "status_code": response["status_code"],
        "headers": response.get("headers", {}),
        "body": response.get("body", ""),
        "payload": payload,
        "fhirVersion": payload.get("fhirVersion") if isinstance(payload, dict) else None,
        "system_interactions": [item.get("code") for item in rest0.get("interaction", [])],
        "resources": resource_map,
    }


def determine_owner_patient(resource: Dict[str, Any], db: WorkDb) -> Optional[str]:
    for key in REFERENCE_OWNER_KEYS:
        element = resource.get(key)
        if isinstance(element, dict):
            ref = element.get("reference")
            parsed = parse_relative_reference(ref) if isinstance(ref, str) else None
            if parsed and parsed[0] == "Patient":
                return parsed[1]

    for key in ("encounter", "context"):
        element = resource.get(key)
        if isinstance(element, dict):
            ref = element.get("reference")
            parsed = parse_relative_reference(ref) if isinstance(ref, str) else None
            if parsed and parsed[0] == "Encounter":
                owner = db.lookup_encounter_patient(parsed[1])
                if owner:
                    return owner
    return None


def log_skipped(
    ctx: Context,
    source_file: str,
    line_number: int,
    reason: str,
    payload: Any,
) -> None:
    ctx.skipped_writer.row(
        {
            "source_file": source_file,
            "line_number": line_number,
            "reason": reason,
            "payload": payload,
        }
    )


def index_patient_selection(ctx: Context, selection: SelectionFilter) -> None:
    source_name = PATIENT_FILE_SPEC["name"]
    declared_source_path = declared_scan_input_path(ctx, PATIENT_FILE_SPEC)
    if not declared_source_path.exists():
        raise FileNotFoundError(f"Patient source file is required but missing: {declared_source_path}")

    if ctx.args.resume and ctx.db.is_scanned(source_name):
        selection.selected_order = ctx.db.get_selected_patient_ids()
        selection.selected_set = set(selection.selected_order)
        selection.total_patient_records = max(ctx.db.count_patient_index_rows(), len(selection.selected_order))
        print(f"[scan] skipping previously indexed {source_name}", flush=True)
    else:
        source_path = materialize_scan_input(ctx, PATIENT_FILE_SPEC)
        try:
            print(f"[scan] indexing {source_name}", flush=True)
            progress = ProgressPrinter(source_name)
            with source_path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(tqdm(handle, desc=source_name, unit="lines"), start=1):
                    progress.maybe(line_number)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        resource = json.loads(line)
                    except json.JSONDecodeError:
                        ctx.stats.data["malformed_line_count"] += 1
                        log_skipped(ctx, source_name, line_number, "malformed_json", line[:500])
                        continue

                    resource_type = str(resource.get("resourceType", PATIENT_FILE_SPEC["resourceType"]))
                    source_id = str(resource.get("id") or stable_fallback_id(resource_type, line[:200]))
                    resource["resourceType"] = resource_type
                    resource["id"] = source_id

                    ctx.stats.data["resources_processed_by_type"][resource_type] += 1
                    ctx.stats.data["resources_processed_by_file"][source_name] += 1

                    patient_ordinal = selection.total_patient_records
                    identifier_value = first_identifier_value(resource)
                    ctx.db.upsert_patient_index(source_id, identifier_value, patient_ordinal)
                    if selection.plan_patient_record(source_id):
                        ctx.db.upsert_selected_patient(resource, identifier_value, patient_ordinal)

                    if line_number % 5000 == 0:
                        ctx.db.commit()

            ctx.db.commit()
            ctx.db.mark_scanned(source_name)
        finally:
            cleanup_staged_scan_input(ctx, PATIENT_FILE_SPEC, source_path)

    if selection.explicit_ids:
        missing_ids = [patient_id for patient_id in selection.explicit_ids if patient_id not in selection.selected_set]
        if missing_ids:
            ctx.stats.note(f"Explicit patient IDs not found in Patient file: {', '.join(missing_ids[:20])}")
        if selection.offset or selection.num_patients is not None:
            ctx.stats.note("Explicit patient IDs were provided, so offset/num-patients were ignored.")
    else:
        ctx.stats.note(
            f"Selected {len(selection.selected_order):,} patients from Patient file order "
            f"with offset={selection.offset:,} and num_patients="
            f"{'all remaining' if selection.num_patients is None else f'{selection.num_patients:,}'}."
        )

    ctx.stats.data["patients_selected"] = len(selection.selected_order)
    ctx.stats.data["selection_offset"] = selection.offset
    ctx.stats.data["selection_num_patients"] = selection.num_patients


def scan_corpus(ctx: Context, selection: SelectionFilter) -> None:
    for spec in FILE_ORDER:
        if spec["kind"] == "patient":
            continue
        source_name = spec["name"]
        declared_source_path = declared_scan_input_path(ctx, spec)
        if not declared_source_path.exists():
            ctx.stats.note(f"Source file missing: {source_name}")
            continue
        if ctx.args.resume and ctx.db.is_scanned(source_name):
            print(f"[scan] skipping previously indexed {source_name}", flush=True)
            continue

        source_path = materialize_scan_input(ctx, spec)
        try:
            print(f"[scan] indexing {source_name}", flush=True)
            progress = ProgressPrinter(source_name)
            with source_path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(tqdm(handle, desc=source_name, unit="lines"), start=1):
                    progress.maybe(line_number)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        resource = json.loads(line)
                    except json.JSONDecodeError:
                        ctx.stats.data["malformed_line_count"] += 1
                        log_skipped(ctx, source_name, line_number, "malformed_json", line[:500])
                        continue

                    resource_type = str(resource.get("resourceType", spec["resourceType"]))
                    source_id = str(resource.get("id") or stable_fallback_id(resource_type, line[:200]))
                    resource["resourceType"] = resource_type
                    resource["id"] = source_id

                    ctx.stats.data["resources_processed_by_type"][resource_type] += 1
                    ctx.stats.data["resources_processed_by_file"][source_name] += 1

                    if spec["kind"] == "shared":
                        ctx.db.upsert_shared_resource(resource_type, source_id, source_name, resource)
                        continue

                    owner_patient = determine_owner_patient(resource, ctx.db)
                    if not owner_patient:
                        if not selection.is_filtered:
                            log_skipped(
                                ctx,
                                source_name,
                                line_number,
                                "owner_not_resolved",
                                {"resourceType": resource_type, "id": source_id},
                            )
                        continue
                    if not selection.accept_owner(owner_patient):
                        continue

                    ctx.db.upsert_patient_resource(owner_patient, resource_type, source_id, source_name, resource)
                    if resource_type == "Encounter":
                        ctx.db.map_encounter_to_patient(source_id, owner_patient)

                    if line_number % 5000 == 0:
                        ctx.db.commit()

            ctx.db.commit()
            ctx.db.mark_scanned(source_name)
        finally:
            cleanup_staged_scan_input(ctx, spec, source_path)

    ctx.stats.data["sqlite_bytes_after_scan"] = ctx.db.file_size_bytes()
    ctx.stats.note("Discovery pass found no practitioner resource file and no observed practitioner references.")


def convert_codeable_to_concept(value: Dict[str, Any]) -> Dict[str, Any]:
    if "coding" in value or "text" in value:
        return value
    return {"coding": [value]}


def convert_to_codeable_reference_from_reference(reference: Dict[str, Any]) -> Dict[str, Any]:
    return {"reference": deepcopy(reference)}


def convert_to_codeable_reference_from_concept(concept: Dict[str, Any]) -> Dict[str, Any]:
    return {"concept": deepcopy(concept)}


def ensure_codeable_concept_array(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [convert_codeable_to_concept(item) if isinstance(item, dict) else item for item in value]
    if isinstance(value, dict):
        return [convert_codeable_to_concept(value)]
    return [value]


def normalize_dosage_arrays(resource: Dict[str, Any], counter: Counter) -> None:
    for dosage_key in ("dosageInstruction", "dosage"):
        value = resource.get(dosage_key)
        if not isinstance(value, list):
            continue
        for dosage in value:
            if isinstance(dosage, dict) and isinstance(dosage.get("maxDosePerPeriod"), dict):
                dosage["maxDosePerPeriod"] = [dosage["maxDosePerPeriod"]]
                counter[f"{dosage_key}.maxDosePerPeriod->array"] += 1


def patch_r4_to_r5(
    resource: Dict[str, Any],
    patient_id: str,
    ctx: Context,
) -> Dict[str, Any]:
    result = deepcopy(resource)
    resource_type = result["resourceType"]
    resource_id = result["id"]

    meta = result.get("meta")
    if isinstance(meta, dict) and meta.get("profile"):
        profiles = meta.pop("profile")
        ctx.conversion_writer.row(
            {
                "patient_id": patient_id,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "warning_type": "meta.profile_stripped",
                "details": json.dumps(profiles, ensure_ascii=True),
            }
        )
        ctx.stats.data["custom_patches_applied"]["meta.profile_stripped"] += 1
        if not meta:
            result.pop("meta", None)

    if resource_type == "Encounter":
        status = result.get("status")
        mapped_status = ENCOUNTER_STATUS_MAP.get(status)
        if mapped_status and mapped_status != status:
            result["status"] = mapped_status
            ctx.conversion_writer.row(
                {
                    "patient_id": patient_id,
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "warning_type": "Encounter.status_mapped",
                    "details": f"{status} -> {mapped_status}",
                }
            )
            ctx.stats.data["custom_patches_applied"]["Encounter.status_mapped"] += 1
        value = result.get("class")
        if isinstance(value, dict):
            result["class"] = [convert_codeable_to_concept(value)]
            ctx.stats.data["custom_patches_applied"]["Encounter.class->array_CodeableConcept"] += 1
        if "period" in result:
            result["actualPeriod"] = result.pop("period")
            ctx.stats.data["custom_patches_applied"]["Encounter.period->actualPeriod"] += 1
        if "hospitalization" in result:
            result["admission"] = result.pop("hospitalization")
            ctx.stats.data["custom_patches_applied"]["Encounter.hospitalization->admission"] += 1
        if "serviceType" in result and isinstance(result["serviceType"], dict):
            result["serviceType"] = [convert_to_codeable_reference_from_concept(result["serviceType"])]
            ctx.stats.data["custom_patches_applied"]["Encounter.serviceType->CodeableReference[]"] += 1

    elif resource_type == "Location":
        if "physicalType" in result:
            result["form"] = result.pop("physicalType")
            ctx.stats.data["custom_patches_applied"]["Location.physicalType->form"] += 1

    elif resource_type == "MedicationRequest":
        if "medicationReference" in result:
            result["medication"] = convert_to_codeable_reference_from_reference(result.pop("medicationReference"))
            ctx.stats.data["custom_patches_applied"]["MedicationRequest.medicationReference->medication"] += 1
        elif "medicationCodeableConcept" in result:
            result["medication"] = convert_to_codeable_reference_from_concept(result.pop("medicationCodeableConcept"))
            ctx.stats.data["custom_patches_applied"]["MedicationRequest.medicationCodeableConcept->medication"] += 1

    elif resource_type == "MedicationAdministration":
        if isinstance(result.get("category"), dict):
            result["category"] = ensure_codeable_concept_array(result["category"])
            ctx.stats.data["custom_patches_applied"]["MedicationAdministration.category->array_CodeableConcept"] += 1
        if "context" in result:
            result["encounter"] = result.pop("context")
            ctx.stats.data["custom_patches_applied"]["MedicationAdministration.context->encounter"] += 1
        if "medicationCodeableConcept" in result:
            result["medication"] = convert_to_codeable_reference_from_concept(result.pop("medicationCodeableConcept"))
            ctx.stats.data["custom_patches_applied"]["MedicationAdministration.medicationCodeableConcept->medication"] += 1
        elif "medicationReference" in result:
            result["medication"] = convert_to_codeable_reference_from_reference(result.pop("medicationReference"))
            ctx.stats.data["custom_patches_applied"]["MedicationAdministration.medicationReference->medication"] += 1
        for old_key, new_key in (
            ("effectiveDateTime", "occurenceDateTime"),
            ("effectivePeriod", "occurencePeriod"),
            ("effectiveTiming", "occurenceTiming"),
        ):
            if old_key in result:
                result[new_key] = result.pop(old_key)
                ctx.stats.data["custom_patches_applied"][f"MedicationAdministration.{old_key}->{new_key}"] += 1

    elif resource_type == "MedicationDispense":
        if "context" in result:
            result["encounter"] = result.pop("context")
            ctx.stats.data["custom_patches_applied"]["MedicationDispense.context->encounter"] += 1
        if "medicationCodeableConcept" in result:
            result["medication"] = convert_to_codeable_reference_from_concept(result.pop("medicationCodeableConcept"))
            ctx.stats.data["custom_patches_applied"]["MedicationDispense.medicationCodeableConcept->medication"] += 1
        elif "medicationReference" in result:
            result["medication"] = convert_to_codeable_reference_from_reference(result.pop("medicationReference"))
            ctx.stats.data["custom_patches_applied"]["MedicationDispense.medicationReference->medication"] += 1

    elif resource_type == "MedicationStatement":
        if "context" in result:
            result["encounter"] = result.pop("context")
            ctx.stats.data["custom_patches_applied"]["MedicationStatement.context->encounter"] += 1
        if "medicationCodeableConcept" in result:
            result["medication"] = convert_to_codeable_reference_from_concept(result.pop("medicationCodeableConcept"))
            ctx.stats.data["custom_patches_applied"]["MedicationStatement.medicationCodeableConcept->medication"] += 1
        elif "medicationReference" in result:
            result["medication"] = convert_to_codeable_reference_from_reference(result.pop("medicationReference"))
            ctx.stats.data["custom_patches_applied"]["MedicationStatement.medicationReference->medication"] += 1
        if "dosageInstruction" in result:
            result["dosage"] = result.pop("dosageInstruction")
            ctx.stats.data["custom_patches_applied"]["MedicationStatement.dosageInstruction->dosage"] += 1
        status = result.get("status")
        mapped = MEDICATION_STATEMENT_STATUS_MAP.get(status)
        if mapped and mapped != status:
            result["status"] = mapped
            ctx.conversion_writer.row(
                {
                    "patient_id": patient_id,
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "warning_type": "MedicationStatement.status_mapped",
                    "details": f"{status} -> {mapped}",
                }
            )
            ctx.stats.data["custom_patches_applied"]["MedicationStatement.status_mapped"] += 1

    elif resource_type == "Procedure":
        if isinstance(result.get("category"), dict):
            result["category"] = ensure_codeable_concept_array(result["category"])
            ctx.stats.data["custom_patches_applied"]["Procedure.category->array_CodeableConcept"] += 1
        for old_key, new_key in (
            ("performedDateTime", "occurrenceDateTime"),
            ("performedPeriod", "occurrencePeriod"),
            ("performedString", "occurrenceString"),
            ("performedAge", "occurrenceAge"),
            ("performedRange", "occurrenceRange"),
            ("performedTiming", "occurrenceTiming"),
        ):
            if old_key in result:
                result[new_key] = result.pop(old_key)
                ctx.stats.data["custom_patches_applied"][f"Procedure.{old_key}->{new_key}"] += 1

    elif resource_type == "Medication":
        for ingredient in result.get("ingredient", []):
            if isinstance(ingredient, dict):
                if "itemReference" in ingredient:
                    ingredient["item"] = convert_to_codeable_reference_from_reference(ingredient.pop("itemReference"))
                    ctx.stats.data["custom_patches_applied"]["Medication.ingredient.itemReference->item"] += 1
                elif "itemCodeableConcept" in ingredient:
                    ingredient["item"] = convert_to_codeable_reference_from_concept(ingredient.pop("itemCodeableConcept"))
                    ctx.stats.data["custom_patches_applied"]["Medication.ingredient.itemCodeableConcept->item"] += 1

    elif resource_type == "Condition":
        if "clinicalStatus" not in result:
            result["clinicalStatus"] = {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                        "code": "unknown",
                    }
                ]
            }
            ctx.conversion_writer.row(
                {
                    "patient_id": patient_id,
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "warning_type": "Condition.clinicalStatus_defaulted",
                    "details": "Added R5-required clinicalStatus=unknown because source R4 resource omitted it.",
                }
            )
            ctx.stats.data["custom_patches_applied"]["Condition.clinicalStatus_defaulted"] += 1

    normalize_dosage_arrays(result, ctx.stats.data["custom_patches_applied"])

    return result


def local_resource_id(resource: Dict[str, Any]) -> str:
    if resource.get("id"):
        return str(resource["id"])
    key = json_dumps(resource)
    return stable_fallback_id(str(resource.get("resourceType", "Resource")), key)


def assemble_patient_graph(
    patient_id: str,
    ctx: Context,
) -> Tuple[Dict[Tuple[str, str], Dict[str, Any]], List[Dict[str, str]]]:
    graph: Dict[Tuple[str, str], Dict[str, Any]] = {}
    unresolved: List[Dict[str, str]] = []

    patient = ctx.db.get_patient(patient_id)
    if patient is None:
        unresolved.append(
            {
                "patient_id": patient_id,
                "source_resource_type": "Patient",
                "source_resource_id": patient_id,
                "reference": relative_ref("Patient", patient_id),
                "phase": "seed",
                "note": "missing patient resource",
            }
        )
        return graph, unresolved

    def add_resource(resource: Dict[str, Any], pending: deque[Tuple[str, str]]) -> None:
        key = (resource["resourceType"], local_resource_id(resource))
        if key not in graph:
            graph[key] = resource
            pending.append(key)

    pending: deque[Tuple[str, str]] = deque()
    add_resource(patient, pending)
    for resource in ctx.db.iter_patient_owned_resources(patient_id):
        add_resource(resource, pending)

    while pending:
        key = pending.popleft()
        resource = graph[key]
        for reference in collect_reference_strings(resource):
            parsed = parse_relative_reference(reference)
            if not parsed:
                if reference.startswith("urn:uuid:") or reference.startswith("#"):
                    continue
                unresolved.append(
                    {
                        "patient_id": patient_id,
                        "source_resource_type": resource["resourceType"],
                        "source_resource_id": local_resource_id(resource),
                        "reference": reference,
                        "phase": "closure",
                        "note": "non-local or unparsable reference",
                    }
                )
                continue
            target_type, target_id = parsed
            target_key = (target_type, target_id)
            if target_key in graph:
                continue
            target = ctx.db.get_resource_for_patient(patient_id, target_type, target_id)
            if target is None:
                unresolved.append(
                    {
                        "patient_id": patient_id,
                        "source_resource_type": resource["resourceType"],
                        "source_resource_id": local_resource_id(resource),
                        "reference": reference,
                        "phase": "closure",
                        "note": "target missing from patient graph",
                    }
                )
                continue
            add_resource(target, pending)
    return graph, unresolved


def build_transaction_bundle(
    patient_id: str,
    graph: Dict[Tuple[str, str], Dict[str, Any]],
    ctx: Context,
) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
    converted: Dict[Tuple[str, str], Dict[str, Any]] = {}
    url_map: Dict[Tuple[str, str], Dict[str, str]] = {}
    shared_count = 0

    for key, resource in sorted(graph.items()):
        patched = patch_r4_to_r5(resource, patient_id, ctx)
        resource_id = local_resource_id(patched)
        patched["id"] = resource_id
        converted[key] = patched
        full_url = f"urn:uuid:{uuid_for_key(relative_ref(key[0], resource_id))}"
        url_map[key] = {
            "resource_id": resource_id,
            "request_url": relative_ref(key[0], resource_id),
            "fullUrl": full_url,
        }
        if key[0] in SHARED_RESOURCE_TYPES:
            shared_count += 1

    def mapper(reference: str) -> Optional[str]:
        parsed = parse_relative_reference(reference)
        if not parsed:
            return None
        mapped = url_map.get(parsed)
        return None if mapped is None else mapped["fullUrl"]

    for patched in converted.values():
        rewrite_reference_strings(patched, mapper)

    unresolved_after: List[Dict[str, str]] = []
    full_urls = {item["fullUrl"] for item in url_map.values()}
    for key, patched in converted.items():
        for reference in collect_reference_strings(patched):
            if reference.startswith("urn:uuid:") and reference not in full_urls:
                unresolved_after.append(
                    {
                        "patient_id": patient_id,
                        "source_resource_type": key[0],
                        "source_resource_id": key[1],
                        "reference": reference,
                        "phase": "bundle_validation",
                        "note": "urn reference not found in bundle entries",
                    }
                )
            elif parse_relative_reference(reference):
                unresolved_after.append(
                    {
                        "patient_id": patient_id,
                        "source_resource_type": key[0],
                        "source_resource_id": key[1],
                        "reference": reference,
                        "phase": "bundle_validation",
                        "note": "relative reference remained after rewrite",
                    }
                )

    entries: List[Dict[str, Any]] = []
    for key in sorted(converted):
        patched = converted[key]
        mapping = url_map[key]
        entries.append(
            {
                "fullUrl": mapping["fullUrl"],
                "resource": patched,
                "request": {
                    "method": "PUT",
                    "url": mapping["request_url"],
                },
            }
        )
        ctx.stats.data["resources_emitted_by_type"][patched["resourceType"]] += 1

    bundle = {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": entries,
    }
    bundle["_shared_count"] = shared_count
    return bundle, unresolved_after


def export_manifest(ctx: Context) -> None:
    path = ctx.logs_dir / "manifest.csv"
    writer = CsvWriter(
        path,
        [
            "patient_id",
            "patient_identifier",
            "patient_ordinal",
            "owned_count",
            "emitted_count",
            "shared_count",
            "unresolved_count",
            "status",
            "bundle_path",
            "note",
        ],
    )
    try:
        for row in ctx.db.iter_manifest_rows():
            writer.row(dict(row))
    finally:
        writer.close()


def purge_emitted_patient_payload(ctx: Context, patient_id: str) -> None:
    if not ctx.args.purge_emitted_patient_data:
        return
    counts = ctx.db.purge_patient_payload(patient_id)
    ctx.stats.data["patient_payload_rows_purged"] += counts["patient_resources"]
    ctx.stats.data["patient_records_purged"] += counts["patient_records"]
    ctx.stats.data["encounter_mappings_purged"] += counts["encounter_mappings"]
    ctx.purged_patients_since_vacuum += 1
    if ctx.purged_patients_since_vacuum >= VACUUM_EVERY_PURGED_PATIENTS:
        ctx.db.incremental_vacuum()
        ctx.purged_patients_since_vacuum = 0


def build_bundles(ctx: Context, selection: SelectionFilter) -> List[Path]:
    emitted_paths: List[Path] = []
    patient_ids = selection.ordered_ids() if selection.is_filtered else [row["patient_id"] for row in ctx.db.iter_manifest_rows()]
    seen: set[str] = set()
    ordered_patient_ids: List[str] = []
    for patient_id in patient_ids:
        if patient_id not in seen:
            seen.add(patient_id)
            ordered_patient_ids.append(patient_id)
    if not ordered_patient_ids:
        ordered_patient_ids = [row["patient_id"] for row in ctx.db.iter_manifest_rows()]

    for patient_id in tqdm(ordered_patient_ids, desc="bundles", unit="patients"):
        output_path = ctx.bundles_dir / f"patient-{patient_id}.transaction.r5.json"
        if output_path.exists() and not ctx.args.force:
            try:
                existing_bundle = json.loads(output_path.read_text(encoding="utf-8"))
                existing_summary = summarize_existing_bundle(existing_bundle)
            except (json.JSONDecodeError, OSError) as exc:
                print(f"[bundle] rebuilding unreadable existing bundle for {patient_id}: {exc}", flush=True)
            else:
                print(f"[bundle] keeping existing bundle for {patient_id}", flush=True)
                emitted_paths.append(output_path)
                ctx.stats.data["bundles_emitted"] += 1
                for resource_type, count in existing_summary["resource_counts"].items():
                    ctx.stats.data["resources_emitted_by_type"][resource_type] += count
                ctx.db.update_manifest(
                    patient_id,
                    emitted_count=existing_summary["emitted_count"],
                    shared_count=existing_summary["shared_count"],
                    unresolved_count=0,
                    status="emitted",
                    bundle_path=str(output_path),
                    note="Reused existing bundle on resume",
                )
                purge_emitted_patient_payload(ctx, patient_id)
                continue

        graph, unresolved = assemble_patient_graph(patient_id, ctx)
        if unresolved:
            for item in unresolved:
                ctx.unresolved_writer.row(item)
            ctx.stats.data["unresolved_reference_count"] += len(unresolved)
            ctx.stats.data["bundles_blocked"] += 1
            ctx.db.update_manifest(
                patient_id,
                unresolved_count=len(unresolved),
                status="blocked_unresolved_refs",
                note="Unresolved references prevented emission",
            )
            continue

        bundle, post_warnings = build_transaction_bundle(patient_id, graph, ctx)
        if post_warnings:
            for item in post_warnings:
                ctx.unresolved_writer.row(item)
            ctx.stats.data["unresolved_reference_count"] += len(post_warnings)
            ctx.stats.data["bundles_blocked"] += 1
            ctx.db.update_manifest(
                patient_id,
                unresolved_count=len(post_warnings),
                status="blocked_unresolved_refs",
                note="Bundle validation found unresolved internal references",
            )
            continue

        shared_count = int(bundle.pop("_shared_count", 0))
        atomic_write_json(output_path, bundle)
        ctx.stats.data["bundles_emitted"] += 1
        emitted_paths.append(output_path)
        ctx.db.update_manifest(
            patient_id,
            emitted_count=len(bundle["entry"]),
            shared_count=shared_count,
            unresolved_count=0,
            status="emitted",
            bundle_path=str(output_path),
            note=None,
        )
        purge_emitted_patient_payload(ctx, patient_id)
    ctx.db.commit()
    if ctx.purged_patients_since_vacuum:
        ctx.db.incremental_vacuum()
        ctx.purged_patients_since_vacuum = 0
    ctx.stats.data["bundle_output_bytes"] = path_size_bytes(ctx.bundles_dir)
    ctx.stats.data["sqlite_bytes_after_build"] = ctx.db.file_size_bytes()
    export_manifest(ctx)
    return emitted_paths


def make_test_namespace() -> Dict[str, str]:
    now = dt.datetime.now(dt.timezone.utc)
    suffix = hashlib.sha256(str(time.time_ns()).encode("utf-8")).hexdigest()[:6]
    return {
        "human": f"codex-mimic-test-{now.strftime('%Y%m%dT%H%M%SZ')}-{suffix}",
        "prefix": f"cmt-{now.strftime('%y%m%d%H%M')}-{suffix}",
    }


def namespaced_test_id(namespace: Dict[str, str], resource_type: str, source_id: str) -> str:
    return f"{namespace['prefix']}-{hash_suffix(f'{resource_type}/{source_id}', 20)}"


def clone_bundle_for_import(
    bundle: Dict[str, Any],
    namespace: Dict[str, str],
    mode: str,
) -> Dict[str, Any]:
    clone = deepcopy(bundle)
    entry_map: Dict[str, Dict[str, str]] = {}

    for entry in clone.get("entry", []):
        resource = entry["resource"]
        resource_type = resource["resourceType"]
        original_id = str(resource.get("id") or "")
        original_request_url = entry.get("request", {}).get("url", "")
        new_id = namespaced_test_id(namespace, resource_type, original_id or original_request_url)
        new_full_url = f"urn:uuid:{uuid_for_key(namespace['human'] + ':' + resource_type + '/' + new_id)}"
        entry_map[entry["fullUrl"]] = {
            "fullUrl": new_full_url,
            "request_url": relative_ref(resource_type, new_id),
            "original_request_url": original_request_url,
            "resource_type": resource_type,
            "new_id": new_id,
        }
        if original_request_url:
            entry_map[original_request_url] = entry_map[entry["fullUrl"]]
        if original_id:
            entry_map[relative_ref(resource_type, original_id)] = entry_map[entry["fullUrl"]]

    for entry in clone.get("entry", []):
        mapping = entry_map[entry["fullUrl"]]
        entry["fullUrl"] = mapping["fullUrl"]
        resource = entry["resource"]
        rewrite_reference_strings(resource, lambda ref: entry_map.get(ref, {}).get("fullUrl"))

        if mode == "put":
            resource["id"] = mapping["new_id"]
            entry["request"]["method"] = "PUT"
            entry["request"]["url"] = mapping["request_url"]
        elif mode == "conditional":
            test_key = relative_ref(mapping["resource_type"], mapping["new_id"])
            if mapping["resource_type"] in FHIR_ID_IDENTIFIER_TYPES:
                append_identifier(resource, TEST_IDENTIFIER_SYSTEM, test_key)
            resource.pop("id", None)
            query = urllib.parse.quote(f"{TEST_IDENTIFIER_SYSTEM}|{test_key}", safe="")
            entry["request"]["method"] = "PUT"
            entry["request"]["url"] = f"{mapping['resource_type']}?identifier={query}"
        else:
            raise ValueError(f"Unsupported import mode: {mode}")

    return clone


def summarize_existing_bundle(bundle: Dict[str, Any]) -> Dict[str, Any]:
    resource_counts: Counter[str] = Counter()
    shared_count = 0
    entries = bundle.get("entry", [])
    for entry in entries:
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")
        if not resource_type:
            continue
        resource_counts[resource_type] += 1
        if resource_type in SHARED_RESOURCE_TYPES:
            shared_count += 1
    return {
        "emitted_count": len(entries),
        "shared_count": shared_count,
        "resource_counts": dict(resource_counts),
    }


def perform_fhir_post(request: urllib.request.Request, timeout_seconds: int) -> Dict[str, Any]:
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                payload = response.read().decode("utf-8")
                return {
                    "status_code": response.status,
                    "headers": dict(response.headers.items()),
                    "body": payload,
                }
        except urllib.error.HTTPError as exc:
            return {
                "status_code": exc.code,
                "headers": dict(exc.headers.items()),
                "body": exc.read().decode("utf-8", errors="replace"),
            }
        except (TimeoutError, socket.timeout) as exc:
            if attempt >= max_attempts:
                return {
                    "status_code": 599,
                    "headers": {},
                    "body": f"Transport timeout while contacting server after {attempt} attempts: {exc}",
                }
        except urllib.error.URLError as exc:
            if attempt >= max_attempts:
                return {
                    "status_code": 598,
                    "headers": {},
                    "body": f"Transport network error while contacting server after {attempt} attempts: {exc}",
                }
        time.sleep(min(5, attempt))
    return {
        "status_code": 598,
        "headers": {},
        "body": "Transport network error while contacting server: retry loop exhausted unexpectedly.",
    }


def post_bundle(server_url: str, bundle: Dict[str, Any], timeout_seconds: int) -> Dict[str, Any]:
    base = sanitize_server_base(server_url)
    body = json.dumps(bundle, ensure_ascii=True).encode("utf-8")
    request = urllib.request.Request(
        base,
        data=body,
        method="POST",
        headers={
            "Accept": FHIR_ACCEPT,
            "Content-Type": FHIR_ACCEPT,
        },
    )
    return perform_fhir_post(request, timeout_seconds)


def post_validate(server_url: str, path_suffix: str, resource: Dict[str, Any], timeout_seconds: int) -> Dict[str, Any]:
    base = sanitize_server_base(server_url)
    body = json.dumps(resource, ensure_ascii=True).encode("utf-8")
    request = urllib.request.Request(
        base + path_suffix,
        data=body,
        method="POST",
        headers={
            "Accept": FHIR_ACCEPT,
            "Content-Type": FHIR_ACCEPT,
        },
    )
    return perform_fhir_post(request, timeout_seconds)


def summarize_operation_outcome(body_text: str) -> Dict[str, Any]:
    try:
        payload = json.loads(body_text)
    except json.JSONDecodeError:
        return {"severity_counts": {}, "error_count": 0, "diagnostics": []}
    issues = payload.get("issue", []) if isinstance(payload, dict) else []
    severity_counts = Counter()
    diagnostics: List[str] = []
    for issue in issues:
        severity = issue.get("severity", "unknown")
        severity_counts[severity] += 1
        diagnostics_text = issue.get("diagnostics")
        if diagnostics_text:
            diagnostics.append(str(diagnostics_text))
    return {
        "severity_counts": dict(severity_counts),
        "error_count": severity_counts.get("error", 0),
        "diagnostics": diagnostics[:12],
    }


def select_sample_bundles(emitted_paths: Sequence[Path], count: int) -> List[Path]:
    by_patient = {}
    for path in emitted_paths:
        name = path.name
        if name.startswith("patient-") and ".transaction.r5.json" in name:
            patient_id = name[len("patient-") : -len(".transaction.r5.json")]
            by_patient[patient_id] = path
    selected: List[Path] = []
    for patient_id in PREFERRED_SAMPLE_PATIENTS:
        path = by_patient.get(patient_id)
        if path and path not in selected:
            selected.append(path)
        if len(selected) >= count:
            return selected
    remaining_paths = sorted(
        (path for path in emitted_paths if path not in selected),
        key=lambda candidate: (candidate.stat().st_size if candidate.exists() else sys.maxsize, candidate.name),
    )
    for path in remaining_paths:
        if path not in selected:
            selected.append(path)
        if len(selected) >= count:
            return selected
    return selected


def parse_bundle_response_counts(body_text: str) -> Dict[str, Any]:
    try:
        payload = json.loads(body_text)
    except json.JSONDecodeError:
        return {}
    counts = Counter()
    statuses = []
    for entry in payload.get("entry", []):
        status = entry.get("response", {}).get("status", "")
        statuses.append(status)
        if status:
            counts[status.split()[0]] += 1
    return {"status_counts": dict(counts), "statuses": statuses}


def collect_representative_resources(sample_paths: Sequence[Path]) -> List[Dict[str, Any]]:
    selected: Dict[str, Dict[str, Any]] = {}
    for path in sample_paths:
        bundle = json.loads(path.read_text(encoding="utf-8"))
        for entry in bundle.get("entry", []):
            resource = entry.get("resource")
            if not isinstance(resource, dict):
                continue
            resource_type = resource.get("resourceType")
            if resource_type and resource_type not in selected:
                selected[resource_type] = {
                    "resource_type": resource_type,
                    "patient_file": path.name,
                    "resource_id": resource.get("id"),
                    "resource": resource,
                }
    return list(selected.values())


def bundle_shared_targets(bundle: Dict[str, Any]) -> Dict[str, List[str]]:
    shared_targets: Dict[str, List[str]] = {resource_type: [] for resource_type in SHARED_RESOURCE_TYPES}
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")
        request_url = entry.get("request", {}).get("url")
        if resource_type in SHARED_RESOURCE_TYPES and isinstance(request_url, str):
            shared_targets[resource_type].append(request_url)
    return {key: values for key, values in shared_targets.items() if values}


def validate_samples(ctx: Context, server_url: str, sample_paths: Sequence[Path]) -> bool:
    preflight: Dict[str, Any] = {"bundle_validation": [], "resource_validation": []}
    has_errors = False
    timeout_seconds = ctx.args.http_timeout

    for path in sample_paths[:1]:
        bundle = json.loads(path.read_text(encoding="utf-8"))
        response = post_validate(server_url, "/Bundle/$validate", bundle, timeout_seconds)
        outcome_summary = summarize_operation_outcome(response["body"])
        bundle_server_limit = (
            response["status_code"] >= 500
            and ("outofmemoryerror" in response["body"].lower() or "java heap space" in response["body"].lower())
        )
        preflight["bundle_validation"].append(
            {
                "bundle_path": str(path),
                "patient_file": path.name,
                "response": response,
                "outcome_summary": outcome_summary,
                "server_limit": bundle_server_limit,
            }
        )
        if bundle_server_limit:
            ctx.stats.note("Bundle/$validate hit a server-side Java heap limit; continuing with resource $validate and transaction import.")
            continue
        if response["status_code"] >= 400 or outcome_summary["error_count"] > 0:
            has_errors = True

    for item in collect_representative_resources(sample_paths):
        resource = item.pop("resource")
        response = post_validate(server_url, f"/{resource['resourceType']}/$validate", resource, timeout_seconds)
        outcome_summary = summarize_operation_outcome(response["body"])
        preflight["resource_validation"].append(
            {
                **item,
                "response": response,
                "outcome_summary": outcome_summary,
            }
        )
        if response["status_code"] >= 400 or outcome_summary["error_count"] > 0:
            has_errors = True

    ctx.import_results["preflight_validation"] = preflight
    return not has_errors


def analyze_shared_resource_reuse(ctx: Context) -> None:
    successful_attempts = [
        attempt
        for attempt in ctx.import_results.get("attempts", [])
        if isinstance(attempt, dict) and attempt.get("response", {}).get("status_code", 500) < 400 and attempt.get("attempt") != "repeat-import"
    ]
    checks: Dict[str, Any] = {"bundle_pairs": []}
    for index in range(len(successful_attempts)):
        left = successful_attempts[index]
        left_targets = left.get("shared_request_targets", {})
        left_statuses = left.get("shared_response_statuses", {})
        for other_index in range(index + 1, len(successful_attempts)):
            right = successful_attempts[other_index]
            right_targets = right.get("shared_request_targets", {})
            overlap: Dict[str, List[str]] = {}
            overlap_statuses: Dict[str, List[Dict[str, Any]]] = {}
            for resource_type in SHARED_RESOURCE_TYPES:
                shared_urls = sorted(set(left_targets.get(resource_type, [])) & set(right_targets.get(resource_type, [])))
                if not shared_urls:
                    continue
                overlap[resource_type] = shared_urls
                overlap_statuses[resource_type] = [
                    {
                        "request_url": request_url,
                        "first_bundle_status": left_statuses.get(request_url),
                        "second_bundle_status": right.get("shared_response_statuses", {}).get(request_url),
                    }
                    for request_url in shared_urls[:20]
                ]
            if overlap:
                checks["bundle_pairs"].append(
                    {
                        "first_patient_file": left.get("patient_file"),
                        "second_patient_file": right.get("patient_file"),
                        "overlap_counts": {resource_type: len(urls) for resource_type, urls in overlap.items()},
                        "overlap_statuses": overlap_statuses,
                    }
                )
                ctx.import_results["shared_resource_reuse"] = checks
                return
    ctx.import_results["shared_resource_reuse"] = checks


def import_samples(ctx: Context, emitted_paths: Sequence[Path]) -> None:
    server_url = ctx.args.server_url or os.environ.get("MIMIC_FHIR_TEST_SERVER_URL")
    if not server_url:
        ctx.import_results["safety_decision"] = "Server import test not requested."
        return

    discovery = discover_server(server_url, ctx.args.http_timeout)
    ctx.import_results["discovery"] = {
        "server_url": sanitize_server_base(server_url),
        "fhirVersion": discovery.get("fhirVersion"),
        "system_interactions": discovery.get("system_interactions"),
        "status_code": discovery.get("status_code"),
        "metadata_url": discovery.get("url"),
        "body": discovery.get("body", ""),
    }

    if discovery.get("status_code", 500) >= 400:
        ctx.import_results["safety_decision"] = (
            "Safe production testing blocked: CapabilityStatement discovery failed or timed out before any write was attempted."
        )
        return

    if discovery.get("fhirVersion") != "5.0.0":
        ctx.import_results["safety_decision"] = "Safe production testing blocked: target server is not advertising FHIR R5."
        return
    if "transaction" not in set(discovery.get("system_interactions", [])):
        ctx.import_results["safety_decision"] = "Safe production testing blocked: target server does not advertise transaction support."
        return

    sample_paths = select_sample_bundles(emitted_paths, ctx.args.import_sample_count)
    if not sample_paths:
        ctx.import_results["safety_decision"] = "Safe production testing blocked: no emitted bundles were available to test."
        return
    if not validate_samples(ctx, server_url, sample_paths):
        ctx.import_results["safety_decision"] = (
            "Safe production testing blocked: preflight $validate checks reported R5 structural errors in the generated samples."
        )
        atomic_write_json(ctx.logs_dir / "import_test_results.json", ctx.import_results)
        return

    namespace = make_test_namespace()
    ctx.import_results["namespace"] = namespace
    ctx.import_results["safety_decision"] = (
        "Minimal writes permitted: only namespaced test resources are sent in transaction bundles. "
        "No delete, no package install, no administrative operations."
    )

    mode = "put"
    first_failure_requires_fallback = False
    first_pass_payloads: List[Tuple[Path, Dict[str, Any], Dict[str, Any]]] = []
    timeout_seconds = ctx.args.http_timeout

    for index, path in enumerate(sample_paths):
        bundle = json.loads(path.read_text(encoding="utf-8"))
        namespaced_bundle = clone_bundle_for_import(bundle, namespace, mode)
        response = post_bundle(server_url, namespaced_bundle, timeout_seconds)
        shared_targets = bundle_shared_targets(namespaced_bundle)
        response_summary = parse_bundle_response_counts(response["body"])
        shared_response_statuses: Dict[str, Optional[str]] = {}
        statuses = response_summary.get("statuses", [])
        shared_request_urls = {
            request_url
            for values in shared_targets.values()
            for request_url in values
        }
        if statuses and response["status_code"] < 400:
            for entry, status in zip(namespaced_bundle.get("entry", []), statuses):
                request_url = entry.get("request", {}).get("url")
                if request_url in shared_request_urls:
                    shared_response_statuses[request_url] = status
        result = {
            "bundle_path": str(path),
            "patient_file": path.name,
            "attempt": index + 1,
            "mode": mode,
            "request_namespace": namespace["human"],
            "response": response,
            "response_summary": response_summary,
            "failure_category": None,
            "shared_request_targets": shared_targets,
            "shared_response_statuses": shared_response_statuses,
        }
        if response["status_code"] >= 400:
            category = classify_import_failure(response["status_code"], response["body"])
            result["failure_category"] = category
            if category == "duplicate/identity issue":
                first_failure_requires_fallback = True
            ctx.import_results["attempts"].append(result)
            break
        ctx.import_results["attempts"].append(result)
        first_pass_payloads.append((path, bundle, namespaced_bundle))

    if first_failure_requires_fallback:
        ctx.import_results["attempts"].append(
            {"note": "Falling back to conditional update mode after PUT-based transaction import failed."}
        )
        ctx.import_results["namespace"] = namespace
        mode = "conditional"
        ctx.import_results["attempts"] = []
        for index, path in enumerate(sample_paths):
            bundle = json.loads(path.read_text(encoding="utf-8"))
            namespaced_bundle = clone_bundle_for_import(bundle, namespace, mode)
            response = post_bundle(server_url, namespaced_bundle, timeout_seconds)
            shared_targets = bundle_shared_targets(namespaced_bundle)
            response_summary = parse_bundle_response_counts(response["body"])
            shared_response_statuses: Dict[str, Optional[str]] = {}
            statuses = response_summary.get("statuses", [])
            shared_request_urls = {
                request_url
                for values in shared_targets.values()
                for request_url in values
            }
            if statuses and response["status_code"] < 400:
                for entry, status in zip(namespaced_bundle.get("entry", []), statuses):
                    request_url = entry.get("request", {}).get("url")
                    if request_url in shared_request_urls:
                        shared_response_statuses[request_url] = status
            result = {
                "bundle_path": str(path),
                "patient_file": path.name,
                "attempt": index + 1,
                "mode": mode,
                "request_namespace": namespace["human"],
                "response": response,
                "response_summary": response_summary,
                "failure_category": None,
                "shared_request_targets": shared_targets,
                "shared_response_statuses": shared_response_statuses,
            }
            if response["status_code"] >= 400:
                result["failure_category"] = classify_import_failure(response["status_code"], response["body"])
            ctx.import_results["attempts"].append(result)

    if ctx.import_results["attempts"]:
        first_path = sample_paths[0]
        bundle = json.loads(first_path.read_text(encoding="utf-8"))
        repeat_bundle = clone_bundle_for_import(bundle, namespace, mode)
        response = post_bundle(server_url, repeat_bundle, timeout_seconds)
        response_summary = parse_bundle_response_counts(response["body"])
        ctx.import_results["attempts"].append(
            {
                "bundle_path": str(first_path),
                "patient_file": first_path.name,
                "attempt": "repeat-import",
                "mode": mode,
                "request_namespace": namespace["human"],
                "response": response,
                "response_summary": response_summary,
                "failure_category": None if response["status_code"] < 400 else classify_import_failure(response["status_code"], response["body"]),
            }
        )
    analyze_shared_resource_reuse(ctx)

    atomic_write_json(ctx.logs_dir / "import_test_results.json", ctx.import_results)


def write_stats(ctx: Context) -> None:
    atomic_write_json(ctx.logs_dir / "stats.json", ctx.stats.as_json())


def write_production_safety_report(ctx: Context) -> None:
    lines = [
        "# Production Safety Report",
        "",
        f"- Generated at: {utc_now()}",
        f"- Script version: {SCRIPT_VERSION}",
        f"- Dataset root: `{ctx.ndjson_dir}`",
        f"- Input mode: `{ctx.input_prep.get('mode', 'unknown')}`",
        f"- IG/package detected: `{ctx.ig_package_dir}`" if ctx.ig_package_dir else "- IG/package detected: none",
        "- MIMIC IG package was used only as offline reference material; nothing was installed into the target server.",
        "- No delete, no bulk import, no package installation, no reindexing, and no administrative server operations were performed.",
        "- Default external reference allowlist: empty.",
        "- Practitioner handling: the discovery pass saw no practitioner resource file and no observed practitioner references in this corpus.",
        "",
        "## Server Discovery",
    ]
    discovery = ctx.import_results.get("discovery") or {}
    if discovery:
        lines.extend(
            [
                f"- Server URL: `{discovery.get('server_url')}`",
                f"- CapabilityStatement status: `{discovery.get('status_code')}`",
                f"- FHIR version: `{discovery.get('fhirVersion')}`",
                f"- System interactions: `{', '.join(discovery.get('system_interactions') or [])}`",
            ]
        )
    else:
        lines.append("- No server discovery was performed.")

    preflight = ctx.import_results.get("preflight_validation") or {}
    lines.extend(["", "## Preflight Validation"])
    if preflight:
        bundle_errors = sum(item.get("outcome_summary", {}).get("error_count", 0) for item in preflight.get("bundle_validation", []))
        resource_errors = sum(item.get("outcome_summary", {}).get("error_count", 0) for item in preflight.get("resource_validation", []))
        lines.append(f"- Bundle validation samples: `{len(preflight.get('bundle_validation', []))}`")
        lines.append(f"- Resource validation samples: `{len(preflight.get('resource_validation', []))}`")
        lines.append(f"- Bundle validation errors: `{bundle_errors}`")
        lines.append(f"- Resource validation errors: `{resource_errors}`")
    else:
        lines.append("- No $validate preflight checks were performed.")

    lines.extend(["", "## Safety Decision", f"- {ctx.import_results.get('safety_decision', 'No decision recorded.')}"])

    namespace = ctx.import_results.get("namespace")
    lines.extend(["", "## Namespaced Test Footprint"])
    if namespace:
        lines.append(f"- Test namespace: `{namespace.get('human')}`")
        lines.append(f"- Test ID prefix: `{namespace.get('prefix')}`")
    else:
        lines.append("- No namespaced writes were attempted.")

    lines.extend(["", "## Import Attempts"])
    attempts = ctx.import_results.get("attempts") or []
    if attempts:
        for attempt in attempts:
            if "note" in attempt:
                lines.append(f"- {attempt['note']}")
                continue
            response = attempt.get("response", {})
            lines.append(
                f"- `{attempt.get('attempt')}` `{attempt.get('patient_file')}` mode=`{attempt.get('mode')}` "
                f"status=`{response.get('status_code')}` failure_category=`{attempt.get('failure_category')}`"
            )
    else:
        lines.append("- No write attempts were made.")

    shared_reuse = ctx.import_results.get("shared_resource_reuse") or {}
    lines.extend(["", "## Shared Resource Reuse"])
    if shared_reuse.get("bundle_pairs"):
        for pair in shared_reuse["bundle_pairs"]:
            counts = ", ".join(f"{key}={value}" for key, value in sorted(pair.get("overlap_counts", {}).items()))
            lines.append(
                f"- `{pair.get('first_patient_file')}` and `{pair.get('second_patient_file')}` reused shared IDs: {counts}"
            )
    else:
        lines.append("- No overlapping shared-resource IDs were observed across the successfully imported sample bundles.")

    atomic_write_text(ctx.logs_dir / "production_safety_report.md", "\n".join(lines) + "\n")


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate one atomic patient-centric R5 transaction bundle per patient from MIMIC NDJSON."
    )
    parser.add_argument("input_folder", help="Repository root, fhir/ directory, or another parent directory containing them.")
    parser.add_argument("--gz", action="store_true", help="Stage and use .ndjson.gz inputs instead of requiring extracted .ndjson files.")
    parser.add_argument("--output-dir", help="Output directory. Defaults to <repo_root>/output")
    parser.add_argument("--num-patients", type=int, default=None, help="Process at most N patients from the selected Patient-file offset.")
    parser.add_argument("--max-patients", dest="num_patients", type=int, help="Deprecated alias for --num-patients.")
    parser.add_argument("--offset", type=int, default=0, help="Start from the 0-based patient ordinal in Patient-file order.")
    parser.add_argument("--patient-ids", default="", help="Comma-separated patient IDs to process.")
    parser.add_argument("--force", action="store_true", help="Rebuild outputs and staging DB from scratch.")
    parser.add_argument("--resume", dest="resume", action="store_true", default=True, help="Resume from prior staging DB if compatible.")
    parser.add_argument("--no-resume", dest="resume", action="store_false", help="Do not reuse prior staging DB state.")
    parser.add_argument(
        "--server-url",
        default=None,
        help="Optional R5 server URL for isolated import tests, for example http://localhost:8080/fhir/.",
    )
    parser.add_argument("--import-sample-count", type=int, default=5, help="How many emitted bundles to test against the server.")
    parser.add_argument("--http-timeout", type=int, default=600, help="HTTP timeout in seconds for $validate and transaction import calls.")
    parser.add_argument(
        "--purge-emitted-patient-data",
        dest="purge_emitted_patient_data",
        action="store_true",
        default=True,
        help="Delete emitted patient payload rows from SQLite after bundle creation to reclaim disk.",
    )
    parser.add_argument(
        "--keep-emitted-patient-data",
        dest="purge_emitted_patient_data",
        action="store_false",
        help="Keep emitted patient payload rows in SQLite instead of purging them.",
    )
    parser.add_argument("--keep-work-db", action="store_true", help="Keep the staging SQLite database after the run.")
    return parser.parse_args(argv)


def build_run_signature(args: argparse.Namespace, ndjson_dir: Path, input_prep: Dict[str, Any]) -> Dict[str, str]:
    explicit_ids = [value.strip() for value in args.patient_ids.split(",") if value.strip()]
    source_mtimes = {
        name: data["mtime_ns"]
        for name, data in sorted((input_prep.get("source_files") or {}).items())
    }
    signature = {
        "script_version": SCRIPT_VERSION,
        "ndjson_dir": str(ndjson_dir.resolve()),
        "input_mode": str(input_prep.get("mode", "unknown")),
        "patient_ids": ",".join(explicit_ids),
        "num_patients": "" if args.num_patients is None else str(args.num_patients),
        "offset": str(args.offset),
        "purge_emitted_patient_data": "1" if args.purge_emitted_patient_data else "0",
        "source_mtimes": json.dumps(source_mtimes, ensure_ascii=True, sort_keys=True),
    }
    signature["signature_hash"] = hash_suffix(json_dumps(signature), 16)
    return signature


def prepare_db(args: argparse.Namespace, output_dir: Path, signature: Dict[str, str]) -> Tuple[WorkDb, Path]:
    db_path = output_dir / "logs" / "staging.sqlite"
    if args.force and db_path.exists():
        db_path.unlink()
    db = WorkDb(db_path)
    db.ensure_meta(signature, resume=args.resume)
    return db, db_path


def remove_work_db_if_needed(db: WorkDb, db_path: Path, keep: bool) -> None:
    db.close()
    if keep:
        return
    for suffix in ("", "-shm", "-wal", "-journal"):
        path = Path(str(db_path) + suffix)
        if path.exists():
            path.unlink()


def main(argv: Sequence[str]) -> int:
    run_started = time.perf_counter()
    args = parse_args(argv)
    if args.offset < 0:
        raise ValueError("--offset must be >= 0")
    if args.num_patients is not None and args.num_patients < 0:
        raise ValueError("--num-patients/--max-patients must be >= 0")
    discovered = detect_dataset_paths(Path(args.input_folder))
    repo_root = discovered["repo_root"]
    discovered_ndjson_dir = discovered["ndjson_dir"]
    ig_package_dir = discovered["ig_package_dir"]
    assert repo_root is not None and discovered_ndjson_dir is not None

    output_dir = Path(args.output_dir).resolve() if args.output_dir else (repo_root / "output").resolve()
    ensure_dir(output_dir)
    phase_started = time.perf_counter()
    ndjson_dir, input_prep = prepare_input_directory(discovered_ndjson_dir, output_dir, args.gz, args.force)
    db, db_path = prepare_db(args, output_dir, build_run_signature(args, ndjson_dir, input_prep))
    ctx = Context(args, repo_root, ndjson_dir, ig_package_dir, output_dir, db, input_prep)
    record_timing(ctx.stats, "input_preparation", time.perf_counter() - phase_started)
    selection = SelectionFilter(args.patient_ids.split(","), args.num_patients, args.offset)

    try:
        print(f"[setup] repo_root={repo_root}", flush=True)
        print(f"[setup] ndjson_dir={ndjson_dir}", flush=True)
        print(f"[setup] input_mode={input_prep.get('mode')}", flush=True)
        if input_prep.get("staged_dir"):
            print(f"[setup] staged_ndjson_dir={input_prep['staged_dir']}", flush=True)
        if ig_package_dir:
            print(f"[setup] ig_package_dir={ig_package_dir}", flush=True)
        print(f"[setup] output_dir={output_dir}", flush=True)

        record_input_preparation(ctx)
        ctx.stats.note(
            f"SQLite staging uses journal_mode=DELETE, temp_store=FILE, cache_size={SQLITE_CACHE_MB}MB, "
            "and incremental vacuum to reduce RAM and reclaim deleted patient payload pages."
        )
        phase_started = time.perf_counter()
        index_patient_selection(ctx, selection)
        record_timing(ctx.stats, "patient_selection_index", time.perf_counter() - phase_started)

        phase_started = time.perf_counter()
        scan_corpus(ctx, selection)
        record_timing(ctx.stats, "corpus_scan", time.perf_counter() - phase_started)
        ctx.db.commit()

        phase_started = time.perf_counter()
        emitted_paths = build_bundles(ctx, selection)
        record_timing(ctx.stats, "bundle_generation", time.perf_counter() - phase_started)
        export_manifest(ctx)

        phase_started = time.perf_counter()
        import_samples(ctx, emitted_paths)
        record_timing(ctx.stats, "server_import_tests", time.perf_counter() - phase_started)

        phase_started = time.perf_counter()
        write_stats(ctx)
        write_production_safety_report(ctx)
        record_timing(ctx.stats, "final_reports", time.perf_counter() - phase_started)
        record_timing(ctx.stats, "total_run", time.perf_counter() - run_started)
        write_stats(ctx)

        print(
            f"[done] emitted={ctx.stats.data['bundles_emitted']} blocked={ctx.stats.data['bundles_blocked']} "
            f"unresolved={ctx.stats.data['unresolved_reference_count']}",
            flush=True,
        )
        return 0
    finally:
        atomic_write_json(ctx.logs_dir / "import_test_results.json", ctx.import_results)
        write_stats(ctx)
        write_production_safety_report(ctx)
        ctx.close()
        remove_work_db_if_needed(ctx.db, db_path, args.keep_work_db)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
