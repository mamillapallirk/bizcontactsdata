# runner_local.py
# Cursor-aware batch runner for OSM pipeline with ETA, time window, + GitHub job summary.
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Set

PROJECT_DIR = Path(__file__).parent
CONFIG_FILE = PROJECT_DIR / "config_osm.py"
ONE_LOCATION_SCRIPT = PROJECT_DIR / "osm_one_location.py"
LOCATIONS_CSV = PROJECT_DIR / "locations.csv"

DEDUPE_FILE = PROJECT_DIR / "state_dedupe.jsonl"
CURSOR_FILE = PROJECT_DIR / "cursor.json"
OUTPUT_DIR = PROJECT_DIR / "outputs"

OUTPUT_DIR.mkdir(exist_ok=True)

def load_seen_ids(path: Path) -> Set[str]:
    seen: Set[str] = set()
    if path.exists():
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    pid = obj.get("place_id")
                    if pid:
                        seen.add(str(pid))
                except Exception:
                    pass
    return seen

def extract_place_ids_from_csv(csv_path: Path) -> Set[str]:
    ids: Set[str] = set()
    if not csv_path.exists():
        return ids
    import pandas as pd
    try:
        df = pd.read_csv(csv_path)
        if "Place ID" in df.columns:
            ids = set(df["Place ID"].dropna().astype(str).tolist())
    except Exception:
        pass
    return ids

def append_seen_ids(path: Path, place_ids: Set[str]) -> None:
    if not place_ids:
        return
    with path.open("a") as f:
        for pid in place_ids:
            f.write(json.dumps({"place_id": pid}) + "\n")

def load_locations() -> List[Tuple[str, float]]:
    """
    Supports either:
      1) location,radius_miles
      2) state,city,radius_miles   (recommended)
    Returns list of (location_string, radius_float).
    """
    rows: List[Tuple[str, float]] = []
    with open(LOCATIONS_CSV, newline="") as f:
        sniffer = csv.Sniffer()
        sample = f.read(2048)
        f.seek(0)
        has_header = False
        try:
            has_header = sniffer.has_header(sample)
            dialect = sniffer.sniff(sample)
        except Exception:
            dialect = csv.excel
        reader = csv.reader(f, dialect)
        header = next(reader) if has_header else None

        def norm(h):
            return [c.strip().lower() for c in h] if h else []

        h = norm(header)
        for cols in reader:
            if not cols:
                continue
            if h:
                m = {h[i]: (cols[i].strip() if i < len(cols) else "") for i in range(len(h))}
                if "location" in m and "radius_miles" in m:
                    loc = m["location"]
                    try:
                        radius = float(m["radius_miles"])
                    except Exception:
                        continue
                elif all(k in m for k in ("state", "city", "radius_miles")):
                    st = m["state"][:2].upper()
                    city = m["city"]
                    loc = f"{city}, {st}"
                    try:
                        radius = float(m["radius_miles"])
                    except Exception:
                        continue
                else:
                    continue
            else:
                radius_str = cols[-1].strip()
                loc = ",".join(c.strip() for c in cols[:-1]).strip()
                try:
                    radius = float(radius_str)
                except Exception:
                    continue
            if loc:
                rows.append((loc, radius))
    return rows

def load_cursor(total_count: int) -> int:
    if not CURSOR_FILE.exists():
        return 0
    try:
        obj = json.loads(CURSOR_FILE.read_text())
        idx = int(obj.get("index", 0))
        prev_total = int(obj.get("total", total_count))
        if prev_total != total_count or idx < 0 or idx >= total_count:
            return 0
        return idx
    except Exception:
        return 0

def save_cursor(index: int, total_count: int) -> None:
    CURSOR_FILE.write_text(json.dumps({"index": index, "total": total_count}))

def patch_config_osm(location: str, radius_miles: float) -> None:
    src = CONFIG_FILE.read_text()
    def repl_line(s, key, val):
        pattern = rf'^{key}\s*=\s*.*?$'
        new = f'{key} = "{val}"' if isinstance(val, str) else f'{key} = {val}'
        return re.sub(pattern, new, s, flags=re.MULTILINE)
    src = repl_line(src, "LOCATION", location)
    src = repl_line(src, "RADIUS_MILES", float(radius_miles))
    CONFIG_FILE.write_text(src)

def run_one(location: str, radius: float) -> Set[str]:
    patch_config_osm(location, radius)
    proc = subprocess.run([sys.executable, str(ONE_LOCATION_SCRIPT)], check=False)
    if proc.returncode != 0:
        print(f"Run failed for {location} ({radius} mi)")
        return set()

    created = []
    for p in PROJECT_DIR.glob("* Miles Radius.csv"):
        created.append(p)
        shutil.move(str(p), OUTPUT_DIR / p.name)

    if not created:
        print("No CSVs found in project root after run.")

    new_ids: Set[str] = set()
    for p in created:
        moved = OUTPUT_DIR / p.name
        new_ids |= extract_place_ids_from_csv(moved)

    time.sleep(1.0)
    return new_ids

def write_job_summary(markdown: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a") as f:
            f.write(markdown + "\n")
    except Exception:
        pass

def main():
    # Time window for a single job (seconds). If 0, falls back to BATCH_SIZE.
    MAX_JOB_SECONDS = int(os.getenv("MAX_JOB_SECONDS", "0"))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))

    locations = load_locations()
    total = len(locations)
    if total == 0:
        msg = "**OSM Places Scan:** locations.csv is empty or malformed."
        print(msg)
        write_job_summary(msg)
        sys.exit(0)

    start_idx = load_cursor(total)
    print(f"Total locations: {total}. Starting at index: {start_idx}. Batch size: {BATCH_SIZE}, Time window: {MAX_JOB_SECONDS}s")

    approx_batches_left = (total - start_idx + max(1, BATCH_SIZE) - 1) // max(1, BATCH_SIZE)
    print(f"Approx. batches left in this pass (size={BATCH_SIZE}): {approx_batches_left}")

    seen = load_seen_ids(DEDUPE_FILE)
    print(f"Loaded {len(seen)} deduped Place IDs.")

    processed = 0
    idx = start_idx
    new_ids_total = 0

    run_start = datetime.utcnow()
    per_loc_times: List[float] = []

    while True:
        # Respect time window
        if MAX_JOB_SECONDS:
            elapsed = (datetime.utcnow() - run_start).total_seconds()
            if elapsed >= MAX_JOB_SECONDS:
                print(f"Time window reached ({int(elapsed)}s). Saving cursor and exiting.")
                break

        # Respect batch size if set (acts as an additional guard)
        if not MAX_JOB_SECONDS and processed >= BATCH_SIZE:
            break

        loc, radius = locations[idx]
        tick_start = datetime.utcnow()
        print(f"=== [{idx+1}/{total}] {loc} ({radius} mi) ===")

        new_ids = run_one(loc, radius)
        new_unique = new_ids - seen
        append_seen_ids(DEDUPE_FILE, new_unique)
        seen |= new_ids

        processed += 1
        new_ids_total += len(new_unique)

        # advance cursor
        idx = (idx + 1) % total

        # ETA
        tick_elapsed = (datetime.utcnow() - tick_start).total_seconds()
        per_loc_times.append(tick_elapsed)
        avg_per = sum(per_loc_times) / max(1, len(per_loc_times))
        completed = (idx if idx != 0 else total)
        remaining = total - completed
        eta_td = timedelta(seconds=int(remaining * avg_per))
        pct = completed / total * 100.0
        print(f"Progress: {pct:.2f}% | Avg/loc: {avg_per:.1f}s | ETA for full pass: {eta_td}")

    # Save next start index
    save_cursor(idx, total)

    # Final summary (console + GH job summary)
    run_elapsed = (datetime.utcnow() - run_start).total_seconds()
    completed = (idx if idx != 0 else total)
    pct = completed / total * 100.0
    summary_md = f"""## OSM Places Scan — Batch Summary
- **Processed this run:** {processed} locations
- **New unique Place IDs added:** {new_ids_total}
- **Elapsed:** {int(run_elapsed)}s
- **Cursor:** {completed} / {total}  (_{pct:.2f}% of list_)
- **Next start index:** {idx}
- **Time window used:** {MAX_JOB_SECONDS or "N/A"}s
- **Outputs:** committed under `outputs/`
"""
    print("\n--- Batch Summary ---")
    print(summary_md)
    write_job_summary(summary_md)

if __name__ == "__main__":
    if sys.version_info < (3, 8):
        print("Python 3.8+ is required.")
        sys.exit(1)
    main()
