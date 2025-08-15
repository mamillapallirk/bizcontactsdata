import json, os, subprocess, sys, time, csv, shutil, re
from pathlib import Path

PROJECT_DIR = Path(__file__).parent
CONFIG_FILE = PROJECT_DIR / "config.py"
ONE_LOCATION_SCRIPT = PROJECT_DIR / "places_one_location.py"
LOCATIONS_CSV = PROJECT_DIR / "locations.csv"
DEDUPE_FILE = PROJECT_DIR / "state_dedupe.jsonl"   # persistent across runs
OUTPUT_DIR = PROJECT_DIR / "outputs"               # where we store the CSVs

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

def set_config(location: str, radius_miles: float):
    txt = CONFIG_FILE.read_text()
    # Replace only LOCATION and RADIUS_MILES lines; leave everything else intact
    def repl_line(src, key, val):
        pattern = rf'^{key}\s*=\s*.*?$'
        if isinstance(val, str):
            new = f'{key} = "{val}"'
        else:
            new = f'{key} = {val}'
        return re.sub(pattern, new, src, flags=re.MULTILINE)

    txt = repl_line(txt, "LOCATION", location)
    txt = repl_line(txt, "RADIUS_MILES", float(radius_miles))
    CONFIG_FILE.write_text(txt)

def run_one(location: str, radius: float, seen_before: set):
    set_config(location, radius)
    # Run your existing script (unchanged)
    proc = subprocess.run([sys.executable, str(ONE_LOCATION_SCRIPT)], check=False)
    if proc.returncode != 0:
        print(f"Run failed for {location} ({radius} mi)")
        return set()

    # Build expected filenames
    city = location.split(",")[0].strip()
    st = location.split(",")[-1].strip()[:2].upper()
    suppliers = f"{st} Suppliers - {city} {st} - {int(radius)} Miles Radius.csv"
    retailers = f"{st} Retailers - {city} {st} - {int(radius)} Miles Radius.csv"

    supp_path = PROJECT_DIR / suppliers
    ret_path  = PROJECT_DIR / retailers

    # Gather new place_ids found in this run
    new_ids = extract_place_ids_from_csv(supp_path) | extract_place_ids_from_csv(ret_path)

    # Move outputs into /outputs for easier tracking
    if supp_path.exists():
        shutil.move(str(supp_path), OUTPUT_DIR / supp_path.name)
    if ret_path.exists():
        shutil.move(str(ret_path), OUTPUT_DIR / ret_path.name)

    # polite pacing
    time.sleep(3)
    return new_ids

def main():
    seen = load_seen_ids(DEDUPE_FILE)
    print(f"Loaded {len(seen)} deduped place_ids.")

    # Process a small batch per run (tune BATCH_SIZE via env if desired)
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))
    processed = 0

    with open(LOCATIONS_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if processed >= BATCH_SIZE:
                break
            location = row["location"].strip()
            radius = float(row["radius_miles"])
            print(f"=== Running {location} ({radius} mi) ===")
            new_ids = run_one(location, radius, seen)
            new_unique = new_ids - seen
            append_seen_ids(DEDUPE_FILE, new_unique)
            seen |= new_ids
            processed += 1
            print(f"Added {len(new_unique)} new ids; total seen {len(seen)}.")

if __name__ == "__main__":
    main()
