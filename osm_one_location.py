import re
import time
import requests
import pandas as pd
from typing import List, Dict, Tuple, Any

from config_osm import (
    LOCATION,
    RADIUS_MILES,
    NOMINATIM_USER_AGENT,
    NOMINATIM_EMAIL,
    OVERPASS_URL,
    RETAIL_OSM_TAGS,
    WHOLESALE_OSM_TAGS,
    OSM_TO_NAICS_RETAIL,
    WHOLESALE_KEYWORD_TO_NAICS,
    DEFAULT_WHOLESALE_NAICS,
)

# -------- Utility --------
def miles_to_meters(mi: float) -> int:
    return int(mi * 1609.344)

def parse_city_state_abbr(location: str) -> Tuple[str, str]:
    s = location.strip().replace("  ", " ").replace(",", " ")
    parts = [p for p in s.split() if p]
    if len(parts) >= 2 and len(parts[-1]) in (2, 3):
        state = parts[-1][:2].upper()
        city = " ".join(parts[:-1])
        return city, state
    if parts and len(parts[-1]) == 2:
        return " ".join(parts[:-1]), parts[-1].upper()
    return s, ""

def build_output_filenames(location: str, radius_miles: float) -> Tuple[str, str]:
    city, st = parse_city_state_abbr(location)
    city_clean = re.sub(r"\s+", " ", city).strip().replace(",", "")
    st_abbr = (st or "").upper()
    base = f"{st_abbr} {{who}} - {city_clean} {st_abbr} - {int(radius_miles)} Miles Radius.csv"
    return base.format(who="Suppliers"), base.format(who="Retailers")

# -------- Geocoding (Nominatim) --------
def geocode_nominatim(q: str) -> Tuple[Any, Any]:
    """Free OSM geocoder. Be polite: max ~1 req/sec."""
    url = "https://nominatim.openstreetmap.org/search"
    headers = {"User-Agent": NOMINATIM_USER_AGENT}
    params = {
        "format": "json",
        "q": q,
        "countrycodes": "us",
        "limit": 1,
        "addressdetails": 0,
        "email": NOMINATIM_EMAIL,
    }
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    time.sleep(1.1)  # respect rate limit
    if not data:
        print(f"[Geocode] ZERO_RESULTS for '{q}'")
        return None, None
    lat = float(data[0]["lat"])
    lon = float(data[0]["lon"])
    return lat, lon

# -------- Overpass (with backoff) --------
def overpass_query(lat: float, lon: float, radius_m: int, tags: List[Tuple[str, str]]) -> List[Dict]:
    parts = []
    for k, v in tags:
        parts.append(f'nwr["{k}"="{v}"](around:{radius_m},{lat},{lon});')
    ql = f"""
    [out:json][timeout:180];
    (
      {"".join(parts)}
    );
    out center tags;
    """
    backoff = 5
    for attempt in range(8):  # up to ~8 retries with exponential backoff
        r = requests.post(OVERPASS_URL, data={"data": ql}, timeout=180)
        if r.status_code == 200:
            try:
                return r.json().get("elements", [])
            except Exception:
                print("[Overpass] JSON parse error; returning empty result")
                return []
        if r.status_code in (429, 502, 503, 504):
            wait = int(r.headers.get("Retry-After", backoff))
            print(f"[Overpass] {r.status_code} throttled/overloaded. Sleeping {wait}s...")
            time.sleep(wait)
            backoff = min(backoff * 2, 120)
            continue
        print(f"[Overpass] HTTP {r.status_code}: {r.text[:200]}")
        time.sleep(3)
        r.raise_for_status()
    print("[Overpass] Max retries exceeded; returning empty result.")
    return []

def extract_center(el: Dict) -> Tuple[Any, Any]:
    if "lat" in el and "lon" in el:
        return el["lat"], el["lon"]
    if "center" in el and "lat" in el["center"] and "lon" in el["center"]:
        return el["center"]["lat"], el["center"]["lon"]
    return None, None

def extract_address(el: Dict) -> str:
    tags = el.get("tags", {})
    if "addr:full" in tags:
        return tags["addr:full"]
    parts = []
    hn = tags.get("addr:housenumber")
    st = tags.get("addr:street")
    city = tags.get("addr:city") or tags.get("addr:town") or tags.get("addr:village")
    state = tags.get("addr:state")
    postcode = tags.get("addr:postcode")
    if hn and st: parts.append(f"{hn} {st}")
    elif st: parts.append(st)
    if city: parts.append(city)
    if state: parts.append(state)
    if postcode: parts.append(postcode)
    return ", ".join([p for p in parts if p])

def extract_phone(tags: Dict) -> str:
    return tags.get("contact:phone") or tags.get("phone") or ""

def extract_website(tags: Dict) -> str:
    return tags.get("contact:website") or tags.get("website") or ""

def infer_segment_and_naics(name: str, tags: Dict, kv_pair: Tuple[str, str] | None, hint_segment: str) -> Tuple[str, str]:
    k, v = kv_pair if kv_pair else ("", "")
    blob = f"{name or ''} " + " ".join([f"{kk}={vv}" for kk, vv in tags.items()])
    blob_l = blob.lower()

    # wholesale signal
    if (k == "shop" and v == "wholesale") or "wholesale" in blob_l or "distributor" in blob_l or "merchant wholesaler" in blob_l:
        return "wholesale", infer_wholesale_naics(blob_l)

    # retail by type mapping
    if (k, v) in OSM_TO_NAICS_RETAIL:
        return "retail", OSM_TO_NAICS_RETAIL[(k, v)]

    # fallback to hinted segment
    return hint_segment, ""

def infer_wholesale_naics(blob_l: str) -> str:
    for key, code in WHOLESALE_KEYWORD_TO_NAICS:
        if key in blob_l:
            return code
    return DEFAULT_WHOLESALE_NAICS

def element_uid(el: Dict) -> str:
    # unique ID across nodes/ways/relations: e.g., n_123, w_456, r_789
    t = el.get("type", "?")[0]  # n/w/r
    i = el.get("id")
    return f"{t}_{i}"

def detect_matched_kv(tags: Dict) -> Tuple[str, str] | None:
    for k, v in WHOLESALE_OSM_TAGS + RETAIL_OSM_TAGS:
        if tags.get(k) == v:
            return (k, v)
    if tags.get("shop") == "wholesale":
        return ("shop", "wholesale")
    return None

def build_type_string(tags: Dict) -> str:
    keep = []
    for key in ["shop", "amenity", "wholesale", "industry", "product", "brand", "operator"]:
        if key in tags:
            keep.append(f"{key}={tags[key]}")
    return ", ".join(keep)

# -------- Pipeline --------
def run_one_location_osm(location: str, radius_miles: float) -> pd.DataFrame:
    radius_m = miles_to_meters(radius_miles)
    lat, lon = geocode_nominatim(location)
    if not lat:
        print(f"❌ Could not find coordinates for {location}")
        return pd.DataFrame()

    seen_ids = set()
    rows = []

    # Retail pass
    retail_elements = overpass_query(lat, lon, radius_m, RETAIL_OSM_TAGS)
    print(f"[OSM] Retail elements fetched: {len(retail_elements)}")
    rows.extend(parse_elements(retail_elements, location, "retail", seen_ids))

    # Wholesale pass
    wholesale_elements = overpass_query(lat, lon, radius_m, WHOLESALE_OSM_TAGS)
    print(f"[OSM] Wholesale elements fetched: {len(wholesale_elements)}")
    rows.extend(parse_elements(wholesale_elements, location, "wholesale", seen_ids))

    return pd.DataFrame(rows)

def parse_elements(elements: List[Dict], location: str, hint_segment: str, seen_ids: set) -> List[Dict]:
    parsed = []
    for el in elements:
        uid = element_uid(el)
        if uid in seen_ids:
            continue
        seen_ids.add(uid)

        tags = el.get("tags", {}) or {}
        name = tags.get("name", "")
        lat, lon = extract_center(el)
        address = extract_address(el)
        phone = extract_phone(tags)
        website = extract_website(tags)

        kv_pair = detect_matched_kv(tags)
        segment, naics = infer_segment_and_naics(name, tags, kv_pair, hint_segment)

        parsed.append({
            "Location": location,
            "Search Keyword": "",
            "Segment": segment,
            "NAICS Code": naics,
            "Business Name": name,
            "Address": address,
            "Phone Number": phone,
            "Website": website,
            "Latitude": lat,
            "Longitude": lon,
            "Rating": "",
            "User Ratings": "",
            "Place ID": uid,            # OSM UID for dedupe
            "Types": build_type_string(tags),
        })
    return parsed

# -------- Main --------
def main():
    df = run_one_location_osm(LOCATION, RADIUS_MILES)
    print(f"[DEBUG] Combined rows: {len(df)}")
    if df.empty:
        print("[DEBUG] No rows. Try a larger radius or a bigger place.")
        return

    wholesalers = df[df["Segment"] == "wholesale"].copy()
    retailers  = df[df["Segment"] == "retail"].copy()
    print(f"[DEBUG] wholesalers={len(wholesalers)} retailers={len(retailers)}")

    suppliers_filename, retailers_filename = build_output_filenames(LOCATION, RADIUS_MILES)
    wholesalers.to_csv(suppliers_filename, index=False)
    retailers.to_csv(retailers_filename, index=False)

    print(f"✅ Saved wholesalers: {suppliers_filename}")
    print(f"✅ Saved retailers:  {retailers_filename}")

if __name__ == "__main__":
    main()
