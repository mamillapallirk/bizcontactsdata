# runner_local.py
import json, os, subprocess, sys, time, csv, shutil, re
from pathlib import Path

PROJECT_DIR = Path(__file__).parent
CONFIG_FILE = PROJECT_DIR / "config.py"
ONE_LOCATION_SCRIPT = PROJECT_DIR / "places_one_location.py"
LOCATIONS_CSV = PROJECT_DIR / "locations.csv"

DEDUPE_FILE = PROJECT_DIR / "state_dedupe.jsonl"   # place_id persistence
CURSOR_FILE = PROJECT_DIR / "cursor.json"          # remembers row index across runs
OUTPUT_DIR = PROJECT_DIR / "outputs"

OUTPUT_DIR.mkdir(exist_ok=True)

def load_seen_ids(path: Path):
    seen = set()
    if path.exists():
        with path.open() as f:
            for line in f:
                try:
                    obj = json.loads(line.strip())
                    pid = obj.get("place_id")
                    if pid:
                        seen.add(pid)
                except Exception:
                    pass
    return seen

def extract_place_ids_from_csv(csv_path: Path):
    ids = set()
    if not csv_path.exists():
        return ids
    import pandas as pd
    df = pd.read_csv(csv_path)
    if "Place ID" in df.columns:
        ids = set(df["Place ID"].dropna().astype(str).tolist())
    return ids

def append_seen_ids(path: Path, place_ids):
    if not place_ids:
        return
    with path.open("a") as f:
        for pid in place_ids:
            f.write(json.dumps({"place_id": pid}) + "\n")

def load_locations():
    """
    Supports either:
      1) location,radius_miles
      2) state,city,radius_miles   (recommended)
    Returns a list of tuples: (location_string, radius_float)
    """
    rows = []
    with open(LOCATIONS_CSV, newline="") as f:
        sniffer = csv.Sniffer()
        sample = f.read(2048)
        f.seek(0)
        has_header = sniffer.has_header(sample)
        dialect = sniffer.sniff(sample)
        reader = csv.reader(f, dialect)
        header = next(reader) if has_header else None

        def norm_header(h):
            return [c.strip().lower() for c in h] if h else []

        h = norm_header(header)
        for cols in reader:
            if not cols:
                continue
            if h:
                m = {h[i]: (cols[i].strip() if i < len(cols) else "") for i in range(len(h))}
                if "location" in m and "radius_miles" in m:
                    loc = m["location"]
                    try:
                        radius = float(m["radius_miles"])
                    except:
                        continue
                elif all(k in m for k in ("state", "city", "radius_miles")):
                    st = m["state"][:2].upper()
                    city = m["city"]
                    loc = f"{city}, {st}"
                    try:
                        radius = float(m["radius_miles"])
                    except:
                        continue
                else:
                    continue
            else:
                radius_str = cols[-1].strip()
                loc = ",".join(c.strip() for c in cols[:-1]).strip()
                try:
                    radius = float(radius_str)
                except:
                    continue
            if loc:
                rows.append((loc, radius))
    return rows

def load_cursor(total_count: int):
    """Return starting index; reset if file missing or total changed."""
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

def save_cursor(index: int, total_count: int):
    CURSOR_FILE.write_text(json.dumps({"index": index, "total": total_count}))

def set_config(location: str, radius_miles: float):
    txt = CONFIG_FILE.read_text()
    def repl_line(src, key, val):
        pattern = rf'^{key}\s*=\s*.*?$'
        new = f'{key} = "{val}"' if isinstance(val, str) else f'{key} = {val}'
        return re.sub(pattern, new, src, flags=re.MULTILINE)
    txt = repl_line(txt, "LOCATION", location)
    txt = repl_line(txt, "RADIUS_MILES", float(radius_miles))
    CONFIG_FILE.write_text(txt)

def run_one(location: str, radius: float):
    set_config(location, radius)
    proc = subprocess.run([sys.executable, str(ONE_LOCATION_SCRIPT)], check=False)
    if proc.returncode != 0:
        print(f"Run failed for {location} ({radius} mi)")
        return set()

    # Move any "* Miles Radius.csv" to outputs/
    created = []
    for p in PROJECT_DIR.glob("* Miles Radius.csv"):
        created.append(p)
        shutil.move(str(p), OUTPUT_DIR / p.name)

    if not created:
        print("No CSVs found in project root after run.")

    # harvest place_ids from moved CSVs
    new_ids = set()
    for p in created:
        moved = OUTPUT_DIR / p.name
        new_ids |= extract_place_ids_from_csv(moved)

    time.sleep(2)  # be polite
    return new_ids

def main():
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))

    locations = load_locations()
    total = len(locations)
    if total == 0:
        print("locations.csv is empty or malformed.")
        sys.exit(0)

    start_idx = load_cursor(total)
    print(f"Total locations: {total}. Starting at index: {start_idx}. Batch size: {BATCH_SIZE}")

    # progress: how many batches to finish a full pass (rough)
    batches_left = (total - start_idx + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Approx. batches to finish current pass: {batches_left}")

    seen = load_seen_ids(DEDUPE_FILE)
    print(f"Loaded {len(seen)} deduped place_ids.")

    processed = 0
    idx = start_idx
    new_ids_total = 0

    while processed < BATCH_SIZE:
        loc, radius = locations[idx]
        print(f"=== [{idx+1}/{total}] {loc} ({radius} mi) ===")

        new_ids = run_one(loc, radius)
        new_unique = new_ids - seen
        append_seen_ids(DEDUPE_FILE, new_unique)
        seen |= new_ids
        processed += 1
        new_ids_total += len(new_unique)

        # advance cursor
        idx = (idx + 1) % total

    # save next starting index
    save_cursor(idx, total)

    # final progress summary
    completed = (idx if idx != 0 else total)
    print(f"\n--- Progress Summary ---")
    print(f"Processed this run: {processed} locations")
    print(f"New unique Place IDs this run: {new_ids_total}")
    print(f"Next start index: {idx}  (completed {completed}/{total} = {completed/total:.1%} of the list)")
    print(f"Outputs: see ./outputs/")

if __name__ == "__main__":
    main()
