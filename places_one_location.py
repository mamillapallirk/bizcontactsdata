import re
import time
import requests
import pandas as pd
from typing import List, Tuple, Dict, Any
from config import (
    API_KEY,
    LOCATION,
    RADIUS_MILES,
    RATE_LIMIT_SECONDS,
    RETAIL_KEYWORDS,
    WHOLESALE_KEYWORDS,
)

# ---- Helpers ----
def miles_to_meters(mi: float) -> int:
    return int(mi * 1609.344)

def parse_city_state_abbr(location: str) -> Tuple[str, str]:
    """Returns (city_str, state_abbr) from 'City, ST' or 'City ST'."""
    s = location.strip().replace("  ", " ")
    s = s.replace(",", " ")
    parts = [p for p in s.split() if p]
    if len(parts) >= 2 and len(parts[-1]) in (2, 3):
        state = parts[-1][:2].upper()
        city = " ".join(parts[:-1])
        return city, state
    if parts and len(parts[-1]) == 2:
        return " ".join(parts[:-1]), parts[-1].upper()
    return s, ""

# ---- NAICS mappings ----
GOOGLE_TYPE_TO_NAICS = {
    "grocery_or_supermarket": "445110",
    "convenience_store": "445120",
    "clothing_store": "448140",
    "department_store": "452210",
    "electronics_store": "443142",
    "furniture_store": "442110",
    "hardware_store": "444130",
    "liquor_store": "445310",
    "pet_store": "453910",
    "pharmacy": "446110",
    "restaurant": "722511",
    "shoe_store": "448210",
    "supermarket": "445110",
}

def detect_wholesale_naics_from_keywords(keywords: str) -> str:
    kw = keywords.lower()
    if "frozen" in kw:
        return "424420"
    if "dairy" in kw:
        return "424430"
    if "seafood" in kw or "fish" in kw:
        return "424460"
    if "meat" in kw or "poultry" in kw:
        return "424470"
    if "fruit" in kw or "vegetable" in kw or "produce" in kw:
        return "424480"
    if "grocery" in kw or "general line" in kw or "food service" in kw:
        return "424410"
    return "424490"

def infer_naics_and_segment(name: str, types: List[str]) -> Tuple[str, str]:
    blob = " ".join(types).lower() + " " + (name or "").lower()
    if "wholesale" in blob or "distributor" in blob or "merchant wholesaler" in blob:
        return detect_wholesale_naics_from_keywords(blob), "wholesale"
    for t in types or []:
        if t in GOOGLE_TYPE_TO_NAICS:
            return GOOGLE_TYPE_TO_NAICS[t], "retail"
    return "", "retail"

# ---- Google APIs ----
def get_coordinates(location_name: str) -> Tuple[Any, Any]:
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": location_name, "components": "country:US", "key": API_KEY}
    resp = requests.get(url, params=params)
    try:
        data = resp.json()
    except Exception:
        print(f"Geocode HTTP error: {resp.status_code} body={resp.text[:200]}")
        return None, None
    status = data.get("status")
    if status != "OK":
        print(f"Geocode failed for '{location_name}': status={status} msg={data.get('error_message')}")
        return None, None
    loc = data["results"][0]["geometry"]["location"]
    return loc["lat"], loc["lng"]

def fetch_places(lat: float, lng: float, keyword: str, radius_meters: int) -> List[Dict]:
    all_places: List[Dict] = []
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{lat},{lng}",
        "radius": radius_meters,
        "keyword": keyword,
        "key": API_KEY,
    }
    while True:
        res = requests.get(url, params=params)
        data = res.json()
        all_places.extend(data.get("results", []))
        if "next_page_token" in data:
            time.sleep(2)
            params["pagetoken"] = data["next_page_token"]
        else:
            break
    return all_places

def get_place_details(place_id: str) -> Dict:
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,formatted_phone_number,website",
        "key": API_KEY,
    }
    r = requests.get(url, params=params)
    return r.json().get("result", {})

# ---- Pipeline ----
def run_one_location(location: str, radius_miles: float) -> pd.DataFrame:
    radius_meters = miles_to_meters(radius_miles)
    lat, lng = get_coordinates(location)
    if not lat:
        print(f"❌ Could not find coordinates for {location}")
        return pd.DataFrame()

    seen_place_ids: set = set()
    rows = []

    # Retail pass
    for kw in RETAIL_KEYWORDS:
        print(f"→ Retail search: {kw}")
        results = fetch_places(lat, lng, kw, radius_meters)
        rows.extend(parse_results(results, location, kw, seen_place_ids))

    # Wholesale pass
    for kw in WHOLESALE_KEYWORDS:
        print(f"→ Wholesale search: {kw}")
        results = fetch_places(lat, lng, kw, radius_meters)
        rows.extend(parse_results(results, location, kw, seen_place_ids))

    return pd.DataFrame(rows)

def parse_results(results: List[Dict], location: str, keyword: str, seen_place_ids: set) -> List[Dict]:
    parsed: List[Dict] = []
    for r in results:
        pid = r.get("place_id")
        if not pid or pid in seen_place_ids:
            continue
        seen_place_ids.add(pid)

        details = get_place_details(pid)
        time.sleep(RATE_LIMIT_SECONDS)

        types = r.get("types", [])
        name = r.get("name", "")
        naics_code, segment = infer_naics_and_segment(name, types)

        parsed.append({
            "Location": location,
            "Search Keyword": keyword,
            "Segment": segment,
            "NAICS Code": naics_code,
            "Business Name": name,
            "Address": details.get("formatted_address") or r.get("vicinity"),
            "Phone Number": details.get("formatted_phone_number"),
            "Website": details.get("website"),
            "Latitude": r["geometry"]["location"]["lat"],
            "Longitude": r["geometry"]["location"]["lng"],
            "Rating": r.get("rating"),
            "User Ratings": r.get("user_ratings_total"),
            "Place ID": pid,
            "Types": ", ".join(types),
        })
    return parsed

def build_filenames(location: str, radius_miles: float) -> Tuple[str, str]:
    city, st = parse_city_state_abbr(location)
    city_clean = re.sub(r"\s+", " ", city).strip()
    city_clean = city_clean.replace(",", "")
    st_abbr = (st or "").upper()
    base = f"{st_abbr} {{who}} - {city_clean} {st_abbr} - {int(radius_miles)} Miles Radius.csv"
    return base.format(who="Suppliers"), base.format(who="Retailers")

def main():
    df = run_one_location(LOCATION, RADIUS_MILES)
    if df.empty:
        print("No results found.")
        return

    wholesalers = df[df["Segment"] == "wholesale"].copy()
    retailers  = df[df["Segment"] == "retail"].copy()

    suppliers_filename, retailers_filename = build_filenames(LOCATION, RADIUS_MILES)

    wholesalers.to_csv(suppliers_filename, index=False)
    retailers.to_csv(retailers_filename, index=False)

    print(f"✅ Saved wholesalers: {suppliers_filename} (rows: {len(wholesalers)})")
    print(f"✅ Saved retailers:  {retailers_filename} (rows: {len(retailers)})")

if __name__ == "__main__":
    main()
