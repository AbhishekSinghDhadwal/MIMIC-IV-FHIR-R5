# MIMIC Patient Bundle Generator

`generate_patient_bundles.py` builds one atomic patient-centric FHIR R5 transaction bundle per patient from the MIMIC-IV FHIR demo corpus.

## Default Usage

Use extracted `.ndjson` files by default:

```powershell
python generate_patient_bundles.py <input_folder>
```

Examples:

```powershell
python generate_patient_bundles.py .
python generate_patient_bundles.py . --max-patients 5
python generate_patient_bundles.py . --patient-ids <patient-id-1>,<patient-id-2>
python generate_patient_bundles.py . --num-patients 100000 --offset 100000
```

## Gzip Input Mode

If you want to use the compressed corpus instead of extracted `.ndjson` files, pass `--gz`:

```powershell
python generate_patient_bundles.py <input_folder> --gz
```

Behavior:

- Default mode requires extracted `.ndjson` files and will fail clearly if they are missing.
- `--gz` reads the `.ndjson.gz` files, expands the full compressed corpus into `output/logs/extracted_ndjson/`, and then runs the normal pipeline from that staged directory.
- The source input folder is never modified in place.
- The staged extracted files are kept for the run, so plan disk capacity for both the compressed source and the fully extracted staging directory.

## Optional Server Import Test

Generation works without any server access. To run the isolated HAPI R5 import test against a local server:

```powershell
python generate_patient_bundles.py <input_folder> --server-url http://localhost:8080/fhir/
```

Useful flags:

- `--output-dir <path>`: write bundles and logs somewhere else.
- `--num-patients N`: build at most `N` patients starting from the selected Patient-file offset.
- `--offset N`: skip the first `N` patients in Patient-file order before selecting the batch.
- `--max-patients N`: deprecated alias for `--num-patients`.
- `--patient-ids a,b,c`: build only specific patients.
- `--force`: rebuild outputs and the staging SQLite database.
- `--resume` / `--no-resume`: reuse or ignore a compatible staging database.
- `--import-sample-count N`: how many emitted bundles to import-test when `--server-url` is set.
- `--http-timeout N`: HTTP timeout in seconds for capability discovery, `$validate`, and transaction import calls.
- `--keep-emitted-patient-data`: keep emitted patient payload rows in SQLite instead of purging them after bundle creation.
- `--keep-work-db`: keep `staging.sqlite` after the run.

## Clean Local End-To-End Demo

If you want a fresh local demo run from scratch:

```powershell
Remove-Item -Recurse -Force .\output -ErrorAction SilentlyContinue
python generate_patient_bundles.py . --server-url http://localhost:8080/fhir/ --force
```

That command rebuilds the SQLite staging database, regenerates the full patient bundle set, and runs the isolated sample import checks against the local HAPI R5 server.

## Production-Scale Batch Runs

For large datasets, run the converter in patient batches so SQLite only stages one batch of patient-owned payloads at a time:

```powershell
python generate_patient_bundles.py . --num-patients 100000 --offset 0
python generate_patient_bundles.py . --num-patients 100000 --offset 100000
python generate_patient_bundles.py . --num-patients 100000 --offset 200000
```

Batch selection order is the order of `MimicPatient.ndjson`, not lexicographic patient ID order.

By default the script also purges each emitted patient's payload rows from SQLite after the bundle file is written, while keeping the lightweight patient index and manifest metadata.

`--num-patients` is the main disk-footprint control for the work database. A `100000`-patient batch will still need far more temporary disk than a `10000`-patient batch, even though RAM stays bounded.

## Outputs

The script creates:

```text
output/
  bundles/
    patient-<id>.transaction.r5.json
  logs/
    manifest.csv
    unresolved_refs.csv
    skipped_resources.ndjson
    conversion_warnings.csv
    import_test_results.json
    stats.json
    production_safety_report.md
```

## What The Script Does

- Streams NDJSON line-by-line and stores only staged resource blobs and ownership maps in SQLite.
- Pre-indexes the Patient file so offset-based batch windows can be selected before the heavy resource files are scanned.
- Reconstructs each patient graph from direct patient references plus indirect encounter ownership.
- Recursively pulls in shared resources needed for atomic closure, especially `Organization`, `Location`, and `Medication`.
- Converts required R4 shapes to R5-safe shapes for HAPI import and logs every custom patch.
- Emits transaction bundles with `urn:uuid` `fullUrl` values and deterministic `PUT` targets.
- Reuses existing bundle files safely on restart when `--force` is not set, and rebuilds manifest/stat summaries from those artifacts.
- Deletes emitted patient payload rows from SQLite by default and uses incremental vacuum so deleted pages can be reclaimed over time.
- In `--gz` mode, expands the full compressed corpus into the managed staging directory before scanning so later passes can read normal `.ndjson` files.
- Uses a unique namespaced ID strategy for production-safe import tests so it never targets non-test resource IDs on the server.

## Production Safety Notes

- The script does read-only capability discovery before any write.
- It does not install the MIMIC IG/package into the target server.
- It does not delete resources, run bulk imports, or perform admin operations.
- Server test writes are limited to a very small number of namespaced resources created by the current run.
- SQLite is configured to favor disk over RAM for temp work and uses a small cache plus incremental vacuum for batch runs.
