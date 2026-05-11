#!/usr/bin/env python3
"""Extract unique FHIR coding triples from MIMIC FHIR NDJSON files.

The script is intentionally dependency-free and optimized for the full
extracted MIMIC FHIR corpus. It streams NDJSON line-by-line, stores aggregate
state in SQLite, and writes the final workbook as a minimal XLSX package.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import tempfile
import time
import zipfile
from html import escape
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


SCRIPT_VERSION = "2026.05.06.1"
DEFAULT_BATCH_SIZE = 25_000
COMMIT_EVERY_LINES = 50_000
SQLITE_CACHE_MB = 32
EXCEL_MAX_ROWS = 1_048_576
UNIQUE_CODES_HEADER = (
    "code_system",
    "code",
    "display",
    "occurrence_count",
    "first_source_file",
    "first_resource_type",
    "first_resource_id",
    "first_json_path",
    "first_seen_line",
)
FILES_HEADER = (
    "file_name",
    "file_path",
    "size_bytes",
    "resource_count",
    "coding_occurrence_count",
    "completed_at",
)
RESOURCE_TYPES_HEADER = ("resource_type", "coding_occurrence_count")
SUMMARY_HEADER = ("metric", "value")


CodingKey = Tuple[str, str, str]
CodingFirstSeen = Tuple[str, str, str, str, int]
CodingBatch = Dict[CodingKey, List[Any]]


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def atomic_write_json(path: Path, value: Dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n")


def temp_xml_path(prefix: str) -> Path:
    handle = tempfile.NamedTemporaryFile(prefix=prefix, suffix=".xml", delete=False)
    path = Path(handle.name)
    handle.close()
    return path


def clean_xml_text(value: str) -> str:
    return "".join(
        char
        for char in value
        if char in ("\t", "\n", "\r") or ord(char) >= 0x20
    )


def xml_escape(value: Any) -> str:
    return escape(clean_xml_text("" if value is None else str(value)))


def stringify_scalar(value: Any) -> Optional[str]:
    if value is None or isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    return text if text else None


def display_scalar(value: Any) -> str:
    if value is None or isinstance(value, (dict, list)):
        return ""
    return str(value).strip()


def candidate_input_roots(input_path: Path) -> List[Path]:
    resolved = input_path.resolve()
    roots = [resolved]
    roots.extend(resolved.parents)
    if resolved.is_dir():
        for depth_1 in resolved.iterdir():
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


def detect_fhir_dir(input_path: Path) -> Path:
    for candidate in candidate_input_roots(input_path):
        if candidate.name.lower() == "fhir" and any(candidate.glob("*.ndjson")):
            return candidate
        child = candidate / "fhir"
        if child.exists() and any(child.glob("*.ndjson")):
            return child
    raise FileNotFoundError(f"Could not locate a fhir/ folder containing .ndjson files under {input_path}")


def iter_ndjson_files(fhir_dir: Path, limit_files: Sequence[str]) -> List[Path]:
    selected = {name.strip() for name in limit_files if name.strip()}
    files = sorted(path for path in fhir_dir.glob("*.ndjson") if path.is_file())
    if selected:
        by_name = {path.name: path for path in files}
        missing = sorted(name for name in selected if name not in by_name)
        if missing:
            raise FileNotFoundError(f"--limit-files requested files not found: {', '.join(missing)}")
        files = [by_name[name] for name in sorted(selected)]
    if not files:
        raise FileNotFoundError(f"No .ndjson files found under {fhir_dir}")
    return files


def iter_codings(node: Any, path: str = "$") -> Iterator[Tuple[str, str, str, str]]:
    if isinstance(node, dict):
        system = stringify_scalar(node.get("system"))
        code = stringify_scalar(node.get("code"))
        if system is not None and code is not None:
            yield system, code, display_scalar(node.get("display")), path
        for key, value in node.items():
            child_path = f"{path}.{key}"
            yield from iter_codings(value, child_path)
    elif isinstance(node, list):
        for item in node:
            yield from iter_codings(item, f"{path}[]")


class WorkDb:
    def __init__(self, path: Path) -> None:
        ensure_dir(path.parent)
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=DELETE")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=FILE")
        self.conn.execute(f"PRAGMA cache_size=-{SQLITE_CACHE_MB * 1024}")
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS run_meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS codes (
              system TEXT NOT NULL,
              code TEXT NOT NULL,
              display TEXT NOT NULL,
              occurrence_count INTEGER NOT NULL,
              first_file TEXT NOT NULL,
              first_resource_type TEXT NOT NULL,
              first_resource_id TEXT NOT NULL,
              first_path TEXT NOT NULL,
              first_seen_line INTEGER NOT NULL,
              PRIMARY KEY (system, code, display)
            );
            CREATE TABLE IF NOT EXISTS source_files (
              file_name TEXT PRIMARY KEY,
              file_path TEXT NOT NULL,
              size_bytes INTEGER NOT NULL,
              completed_at TEXT NOT NULL,
              resource_count INTEGER NOT NULL,
              coding_occurrence_count INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS resource_type_counts (
              resource_type TEXT PRIMARY KEY,
              coding_occurrence_count INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_codes_system_code ON codes(system, code);
            CREATE INDEX IF NOT EXISTS idx_codes_count ON codes(occurrence_count DESC);
            """
        )
        self.conn.commit()

    def ensure_meta(self, expected: Dict[str, str], resume: bool) -> None:
        self.init_schema()
        rows = {row["key"]: row["value"] for row in self.conn.execute("SELECT key, value FROM run_meta")}
        if not rows:
            self.write_meta(expected)
            return
        if rows != expected:
            if resume:
                raise RuntimeError(
                    "Existing extraction DB was created with different inputs or options. "
                    "Use --force to rebuild it for this run."
                )
            self.reset()
            self.init_schema()
            self.write_meta(expected)

    def write_meta(self, meta: Dict[str, str]) -> None:
        for key, value in meta.items():
            self.conn.execute("INSERT OR REPLACE INTO run_meta(key, value) VALUES (?, ?)", (key, value))
        self.conn.commit()

    def reset(self) -> None:
        self.conn.executescript(
            """
            DROP TABLE IF EXISTS run_meta;
            DROP TABLE IF EXISTS codes;
            DROP TABLE IF EXISTS source_files;
            DROP TABLE IF EXISTS resource_type_counts;
            """
        )
        self.conn.commit()

    def file_completed(self, file_name: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM source_files WHERE file_name = ?", (file_name,)).fetchone()
        return row is not None

    def flush_codes(self, batch: CodingBatch) -> None:
        if not batch:
            return
        rows = [
            (
                system,
                code,
                display,
                int(values[0]),
                values[1][0],
                values[1][1],
                values[1][2],
                values[1][3],
                values[1][4],
            )
            for (system, code, display), values in batch.items()
        ]
        self.conn.executemany(
            """
            INSERT INTO codes(
              system, code, display, occurrence_count,
              first_file, first_resource_type, first_resource_id, first_path, first_seen_line
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(system, code, display) DO UPDATE SET
              occurrence_count = occurrence_count + excluded.occurrence_count
            """,
            rows,
        )
        batch.clear()

    def add_resource_type_counts(self, counts: Dict[str, int]) -> None:
        if not counts:
            return
        self.conn.executemany(
            """
            INSERT INTO resource_type_counts(resource_type, coding_occurrence_count)
            VALUES (?, ?)
            ON CONFLICT(resource_type) DO UPDATE SET
              coding_occurrence_count = coding_occurrence_count + excluded.coding_occurrence_count
            """,
            sorted(counts.items()),
        )

    def mark_file_complete(
        self,
        path: Path,
        resource_count: int,
        coding_occurrence_count: int,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO source_files(
              file_name, file_path, size_bytes, completed_at, resource_count, coding_occurrence_count
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                path.name,
                str(path.resolve()),
                path.stat().st_size,
                utc_now(),
                resource_count,
                coding_occurrence_count,
            ),
        )

    def commit(self) -> None:
        self.conn.commit()


def add_to_batch(
    batch: CodingBatch,
    key: CodingKey,
    first_seen: CodingFirstSeen,
) -> None:
    existing = batch.get(key)
    if existing is None:
        batch[key] = [1, first_seen]
    else:
        existing[0] += 1


def scan_file(
    db: WorkDb,
    path: Path,
    *,
    limit_lines: Optional[int],
    batch_size: int,
) -> Dict[str, int]:
    batch: CodingBatch = {}
    resource_type_counts: Dict[str, int] = {}
    resource_count = 0
    coding_occurrence_count = 0
    started = time.perf_counter()
    print(f"[scan] indexing {path.name}", flush=True)

    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if limit_lines is not None and line_number > limit_lines:
                break
            line = line.strip()
            if not line:
                continue
            try:
                resource = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"[warn] {path.name}:{line_number} malformed JSON skipped: {exc}", flush=True)
                continue

            resource_count += 1
            resource_type = stringify_scalar(resource.get("resourceType")) or "Unknown"
            resource_id = stringify_scalar(resource.get("id")) or ""
            resource_coding_count = 0

            for system, code, display, json_path in iter_codings(resource):
                key = (system, code, display)
                first_seen = (path.name, resource_type, resource_id, json_path, line_number)
                add_to_batch(batch, key, first_seen)
                coding_occurrence_count += 1
                resource_coding_count += 1
                if len(batch) >= batch_size:
                    db.flush_codes(batch)
                    db.commit()

            if resource_coding_count:
                resource_type_counts[resource_type] = resource_type_counts.get(resource_type, 0) + resource_coding_count

            if line_number % COMMIT_EVERY_LINES == 0:
                db.flush_codes(batch)
                db.add_resource_type_counts(resource_type_counts)
                resource_type_counts.clear()
                db.commit()
                print(
                    f"[scan] {path.name}: {line_number:,} lines, "
                    f"{coding_occurrence_count:,} coding occurrences",
                    flush=True,
                )

    db.flush_codes(batch)
    db.add_resource_type_counts(resource_type_counts)
    db.mark_file_complete(path, resource_count, coding_occurrence_count)
    db.commit()
    elapsed = time.perf_counter() - started
    print(
        f"[scan] completed {path.name}: resources={resource_count:,} "
        f"codings={coding_occurrence_count:,} elapsed={elapsed:.1f}s",
        flush=True,
    )
    return {"resource_count": resource_count, "coding_occurrence_count": coding_occurrence_count}


def col_name(index: int) -> str:
    value = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        value = chr(65 + remainder) + value
    return value


def write_cell(handle: Any, row_number: int, col_number: int, value: Any) -> None:
    ref = f"{col_name(col_number)}{row_number}"
    if isinstance(value, int):
        handle.write(f'<c r="{ref}"><v>{value}</v></c>')
        return
    text = "" if value is None else str(value)
    preserve = ' xml:space="preserve"' if text != text.strip() else ""
    handle.write(f'<c r="{ref}" t="inlineStr"><is><t{preserve}>{xml_escape(text)}</t></is></c>')


def write_sheet_xml(path: Path, rows: Iterable[Sequence[Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n')
        handle.write('<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">\n')
        handle.write("<sheetData>\n")
        for row_number, row in enumerate(rows, start=1):
            handle.write(f'<row r="{row_number}">')
            for col_number, value in enumerate(row, start=1):
                write_cell(handle, row_number, col_number, value)
            handle.write("</row>\n")
        handle.write("</sheetData>\n</worksheet>\n")


def db_scalar(conn: sqlite3.Connection, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    return 0 if row is None else int(row[0] or 0)


def summary_rows(conn: sqlite3.Connection, fhir_dir: Path, started_at: str, finished_at: str) -> Iterator[Sequence[Any]]:
    total_occurrences = db_scalar(conn, "SELECT COALESCE(SUM(occurrence_count), 0) FROM codes")
    unique_count = db_scalar(conn, "SELECT COUNT(*) FROM codes")
    file_count = db_scalar(conn, "SELECT COUNT(*) FROM source_files")
    resource_count = db_scalar(conn, "SELECT COALESCE(SUM(resource_count), 0) FROM source_files")
    yield SUMMARY_HEADER
    yield ("script_version", SCRIPT_VERSION)
    yield ("input_fhir_dir", str(fhir_dir.resolve()))
    yield ("started_at", started_at)
    yield ("finished_at", finished_at)
    yield ("source_files_completed", file_count)
    yield ("resources_scanned", resource_count)
    yield ("coding_occurrences", total_occurrences)
    yield ("unique_system_code_display_groupings", unique_count)


def unique_code_rows(
    conn: sqlite3.Connection,
    *,
    limit: int,
    offset: int,
) -> Iterator[Sequence[Any]]:
    yield UNIQUE_CODES_HEADER
    cursor = conn.execute(
        """
        SELECT
          system, code, display, occurrence_count, first_file,
          first_resource_type, first_resource_id, first_path, first_seen_line
        FROM codes
        ORDER BY system, code, display
        LIMIT ? OFFSET ?
        """,
        (limit, offset),
    )
    for row in cursor:
        yield (
            row["system"],
            row["code"],
            row["display"],
            int(row["occurrence_count"]),
            row["first_file"],
            row["first_resource_type"],
            row["first_resource_id"],
            row["first_path"],
            int(row["first_seen_line"]),
        )


def file_rows(conn: sqlite3.Connection) -> Iterator[Sequence[Any]]:
    yield FILES_HEADER
    cursor = conn.execute(
        """
        SELECT file_name, file_path, size_bytes, resource_count, coding_occurrence_count, completed_at
        FROM source_files
        ORDER BY file_name
        """
    )
    for row in cursor:
        yield (
            row["file_name"],
            row["file_path"],
            int(row["size_bytes"]),
            int(row["resource_count"]),
            int(row["coding_occurrence_count"]),
            row["completed_at"],
        )


def resource_type_rows(conn: sqlite3.Connection) -> Iterator[Sequence[Any]]:
    yield RESOURCE_TYPES_HEADER
    cursor = conn.execute(
        """
        SELECT resource_type, coding_occurrence_count
        FROM resource_type_counts
        ORDER BY coding_occurrence_count DESC, resource_type
        """
    )
    for row in cursor:
        yield (row["resource_type"], int(row["coding_occurrence_count"]))


def workbook_xml(sheet_names: Sequence[str]) -> str:
    sheets = "\n".join(
        f'<sheet name="{xml_escape(name)}" sheetId="{idx}" r:id="rId{idx}"/>'
        for idx, name in enumerate(sheet_names, start=1)
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">\n'
        f"<sheets>\n{sheets}\n</sheets>\n"
        "</workbook>\n"
    )


def workbook_rels_xml(sheet_count: int) -> str:
    rels = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">',
    ]
    for idx in range(1, sheet_count + 1):
        rels.append(
            f'<Relationship Id="rId{idx}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            f'Target="worksheets/sheet{idx}.xml"/>'
        )
    rels.append(
        f'<Relationship Id="rId{sheet_count + 1}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
    )
    rels.append("</Relationships>")
    return "\n".join(rels) + "\n"


def content_types_xml(sheet_count: int) -> str:
    overrides = "\n".join(
        f'<Override PartName="/xl/worksheets/sheet{idx}.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for idx in range(1, sheet_count + 1)
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">\n'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>\n'
        '<Default Extension="xml" ContentType="application/xml"/>\n'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>\n'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>\n'
        f"{overrides}\n"
        "</Types>\n"
    )


def root_rels_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">\n'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>\n'
        "</Relationships>\n"
    )


def styles_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">\n'
        '<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>\n'
        '<fills count="1"><fill><patternFill patternType="none"/></fill></fills>\n'
        '<borders count="1"><border/></borders>\n'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>\n'
        '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>\n'
        "</styleSheet>\n"
    )


def write_xlsx(db_path: Path, fhir_dir: Path, xlsx_path: Path, started_at: str, finished_at: str) -> Dict[str, Any]:
    ensure_dir(xlsx_path.parent)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    unique_count = db_scalar(conn, "SELECT COUNT(*) FROM codes")
    unique_data_rows_per_sheet = EXCEL_MAX_ROWS - 1
    unique_sheet_count = max(1, (unique_count + unique_data_rows_per_sheet - 1) // unique_data_rows_per_sheet)
    unique_sheet_names = [
        "UniqueCodes" if unique_sheet_count == 1 else f"UniqueCodes{i}"
        for i in range(1, unique_sheet_count + 1)
    ]
    sheet_names = ["Summary", *unique_sheet_names, "Files", "ResourceTypes"]

    tmp_paths: List[Path] = []
    try:
        with zipfile.ZipFile(xlsx_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            zf.writestr("[Content_Types].xml", content_types_xml(len(sheet_names)))
            zf.writestr("_rels/.rels", root_rels_xml())
            zf.writestr("xl/workbook.xml", workbook_xml(sheet_names))
            zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml(len(sheet_names)))
            zf.writestr("xl/styles.xml", styles_xml())

            sheet_index = 1
            for rows in (summary_rows(conn, fhir_dir, started_at, finished_at),):
                tmp = temp_xml_path("mimic_codes_summary_")
                tmp_paths.append(tmp)
                write_sheet_xml(tmp, rows)
                zf.write(tmp, f"xl/worksheets/sheet{sheet_index}.xml")
                sheet_index += 1

            for part_index in range(unique_sheet_count):
                offset = part_index * unique_data_rows_per_sheet
                tmp = temp_xml_path("mimic_codes_unique_")
                tmp_paths.append(tmp)
                write_sheet_xml(
                    tmp,
                    unique_code_rows(conn, limit=unique_data_rows_per_sheet, offset=offset),
                )
                zf.write(tmp, f"xl/worksheets/sheet{sheet_index}.xml")
                sheet_index += 1

            for prefix, rows in (
                ("files", file_rows(conn)),
                ("resource_types", resource_type_rows(conn)),
            ):
                tmp = temp_xml_path(f"mimic_codes_{prefix}_")
                tmp_paths.append(tmp)
                write_sheet_xml(tmp, rows)
                zf.write(tmp, f"xl/worksheets/sheet{sheet_index}.xml")
                sheet_index += 1
    finally:
        conn.close()
        for tmp in tmp_paths:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass
    return {"xlsx_path": str(xlsx_path.resolve()), "unique_code_sheets": unique_sheet_count}


def build_run_meta(args: argparse.Namespace, fhir_dir: Path, files: Sequence[Path]) -> Dict[str, str]:
    limit_lines = "" if args.limit_lines is None else str(args.limit_lines)
    return {
        "script_version": SCRIPT_VERSION,
        "fhir_dir": str(fhir_dir.resolve()),
        "limit_files": ",".join(path.name for path in files),
        "limit_lines": limit_lines,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract unique FHIR Coding system/code/display groupings from MIMIC NDJSON."
    )
    parser.add_argument("input_folder", help="FHIR folder, dataset root, or parent containing a fhir/ directory.")
    parser.add_argument("--output-dir", help="Output directory. Defaults to <repo>/output/code_extract.")
    parser.add_argument("--force", action="store_true", help="Delete prior extraction DB/output before running.")
    parser.add_argument("--resume", dest="resume", action="store_true", default=True, help="Resume completed files.")
    parser.add_argument("--no-resume", dest="resume", action="store_false", help="Do not skip completed files.")
    parser.add_argument(
        "--limit-files",
        default="",
        help="Comma-separated .ndjson file names to scan for testing, for example MimicLocation.ndjson.",
    )
    parser.add_argument("--limit-lines", type=int, default=None, help="Testing-only maximum lines per selected file.")
    parser.add_argument("--xlsx-name", default="mimic_unique_codes.xlsx", help="Output XLSX file name.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def remove_existing_outputs(output_dir: Path, xlsx_name: str) -> None:
    db_path = output_dir / "code_extract.sqlite"
    stats_path = output_dir / "code_extract_stats.json"
    xlsx_path = output_dir / xlsx_name
    for path in (db_path, stats_path, xlsx_path):
        if path.exists():
            path.unlink()
    for suffix in ("-journal", "-wal", "-shm"):
        path = Path(str(db_path) + suffix)
        if path.exists():
            path.unlink()


def main(argv: Optional[Sequence[str]] = None) -> int:
    started = time.perf_counter()
    started_at = utc_now()
    args = parse_args(argv)
    if args.limit_lines is not None and args.limit_lines < 0:
        raise ValueError("--limit-lines must be >= 0")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be > 0")

    repo_root = Path(__file__).resolve().parent
    fhir_dir = detect_fhir_dir(Path(args.input_folder))
    limit_files = [name.strip() for name in args.limit_files.split(",") if name.strip()]
    files = iter_ndjson_files(fhir_dir, limit_files)
    output_dir = Path(args.output_dir).resolve() if args.output_dir else (repo_root / "output" / "code_extract").resolve()
    xlsx_path = output_dir / args.xlsx_name
    db_path = output_dir / "code_extract.sqlite"
    stats_path = output_dir / "code_extract_stats.json"

    ensure_dir(output_dir)
    if args.force:
        remove_existing_outputs(output_dir, args.xlsx_name)

    db = WorkDb(db_path)
    run_meta = build_run_meta(args, fhir_dir, files)
    db.ensure_meta(run_meta, resume=args.resume)

    print(f"[setup] fhir_dir={fhir_dir}", flush=True)
    print(f"[setup] output_dir={output_dir}", flush=True)
    print(f"[setup] files={len(files)}", flush=True)
    print(f"[setup] input_mode=ndjson", flush=True)

    scanned_files = 0
    skipped_files = 0
    try:
        for path in files:
            if args.resume and db.file_completed(path.name):
                skipped_files += 1
                print(f"[scan] skipping previously completed {path.name}", flush=True)
                continue
            scan_file(db, path, limit_lines=args.limit_lines, batch_size=args.batch_size)
            scanned_files += 1
    finally:
        db.commit()
        db.close()

    finished_at = utc_now()
    xlsx_info = write_xlsx(db_path, fhir_dir, xlsx_path, started_at, finished_at)

    conn = sqlite3.connect(db_path)
    try:
        unique_count = db_scalar(conn, "SELECT COUNT(*) FROM codes")
        occurrence_count = db_scalar(conn, "SELECT COALESCE(SUM(occurrence_count), 0) FROM codes")
        completed_files = db_scalar(conn, "SELECT COUNT(*) FROM source_files")
        resource_count = db_scalar(conn, "SELECT COALESCE(SUM(resource_count), 0) FROM source_files")
    finally:
        conn.close()

    stats = {
        "script_version": SCRIPT_VERSION,
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "fhir_dir": str(fhir_dir.resolve()),
        "output_dir": str(output_dir),
        "db_path": str(db_path),
        "xlsx_path": str(xlsx_path),
        "input_mode": "ndjson",
        "files_selected": len(files),
        "files_scanned_this_run": scanned_files,
        "files_skipped_resume": skipped_files,
        "files_completed_total": completed_files,
        "resources_scanned": resource_count,
        "coding_occurrences": occurrence_count,
        "unique_system_code_display_groupings": unique_count,
        "unique_code_sheets": xlsx_info["unique_code_sheets"],
        "limit_files": [path.name for path in files] if limit_files else [],
        "limit_lines": args.limit_lines,
    }
    atomic_write_json(stats_path, stats)
    print(
        f"[done] unique={unique_count:,} occurrences={occurrence_count:,} "
        f"files_completed={completed_files:,} xlsx={xlsx_path}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
