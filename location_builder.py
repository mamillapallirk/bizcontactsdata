# build_locations_from_census_places.py
import csv, re, time, requests

DEFAULT_RADIUS_MILES = 25

STATE_FIPS = {
    "01": ("AL","Alabama"), "02": ("AK","Alaska"), "04": ("AZ","Arizona"),
    "05": ("AR","Arkansas"), "06": ("CA","California"), "08": ("CO","Colorado"),
    "09": ("CT","Connecticut"), "10": ("DE","Delaware"), "11": ("DC","District of Columbia"),
    "12": ("FL","Florida"), "13": ("GA","Georgia"), "15": ("HI","Hawaii"),
    "16": ("ID","Idaho"), "17": ("IL","Illinois"), "18": ("IN","Indiana"),
    "19": ("IA","Iowa"), "20": ("KS","Kansas"), "21": ("KY","Kentucky"),
    "22": ("LA","Louisiana"), "23": ("ME","Maine"), "24": ("MD","Maryland"),
    "25": ("MA","Massachusetts"), "26": ("MI","Michigan"), "27": ("MN","Minnesota"),
    "28": ("MS","Mississippi"), "29": ("MO","Missouri"), "30": ("MT","Montana"),
    "31": ("NE","Nebraska"), "32": ("NV","Nevada"), "33": ("NH","New Hampshire"),
    "34": ("NJ","New Jersey"), "35": ("NM","New Mexico"), "36": ("NY","New York"),
    "37": ("NC","North Carolina"), "38": ("ND","North Dakota"), "39": ("OH","Ohio"),
    "40": ("OK","Oklahoma"), "41": ("OR","Oregon"), "42": ("PA","Pennsylvania"),
    "44": ("RI","Rhode Island"), "45": ("SC","South Carolina"), "46": ("SD","South Dakota"),
    "47": ("TN","Tennessee"), "48": ("TX","Texas"), "49": ("UT","Utah"),
    "50": ("VT","Vermont"), "51": ("VA","Virginia"), "53": ("WA","Washington"),
    "54": ("WV","West Virginia"), "55": ("WI","Wisconsin"), "56": ("WY","Wyoming"),
}

CENSUS_URL = "https://api.census.gov/data/2023/acs/acs5?get=NAME&for=place:*&in=state:{}"

def clean_place_name(raw_name: str, state_full: str) -> str:
    name = re.sub(rf",\s*{re.escape(state_full)}$", "", raw_name).strip()
    name = re.sub(r"\s+(city|town|village|CDP|borough|municipality|urban county|consolidated city)$",
                  "", name, flags=re.I)
    return name

def fetch_places_for_state(fips: str, abbr: str, full: str):
    r = requests.get(CENSUS_URL.format(fips), timeout=30)
    r.raise_for_status()
    data = r.json()
    rows = data[1:]  # skip header
    for NAME, state_code, place_code in rows:
        place = clean_place_name(NAME, full)
        if not place or place.lower().startswith("balance of"):
            continue
        yield abbr, place

def main():
    out = []
    for fips, (abbr, full) in STATE_FIPS.items():
        try:
            count = 0
            for ab, place in fetch_places_for_state(fips, abbr, full):
                out.append((ab, place, DEFAULT_RADIUS_MILES))
                count += 1
            print(f"{abbr}: {count} places")
            time.sleep(0.4)  # be polite
        except Exception as e:
            print(f"Failed for {abbr}: {e}")

    # de-dup while preserving order
    seen = set()
    deduped = []
    for st, city, r in out:
        key = (st, city.lower())
        if key not in seen:
            seen.add(key)
            deduped.append((st, city, r))

    with open("locations.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["state","city","radius_miles"])
        for st, city, r in deduped:
            w.writerow([st, city, int(r)])

    print(f"Wrote {len(deduped)} rows to locations.csv")

if __name__ == "__main__":
    main()
