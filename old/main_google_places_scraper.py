import requests
import time
import pandas as pd
from config import API_KEY, STATE_CITIES, RETAIL_KEYWORDS

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

def get_coordinates(location_name):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": location_name,
        "components": "country:US",   # bias to US
        "key": API_KEY
    }
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

def fetch_places(lat, lng, keyword):
    all_places = []
    url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        'location': f"{lat},{lng}",
        'radius': 50000,
        'keyword': keyword,
        'key': API_KEY
    }

    while True:
        res = requests.get(url, params=params)
        data = res.json()
        all_places.extend(data.get('results', []))
        if 'next_page_token' in data:
            time.sleep(2)
            params['pagetoken'] = data['next_page_token']
        else:
            break

    return all_places

def get_place_details(place_id):
    url = f"https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        'place_id': place_id,
        'fields': 'name,formatted_address,formatted_phone_number,website',
        'key': API_KEY
    }
    response = requests.get(url, params=params)
    return response.json().get('result', {})

def get_naics_code_from_types(types, name=""):
    keywords = ' '.join(types).lower() + ' ' + name.lower()

    if "wholesale" in keywords or "distributor" in keywords:
        if "frozen" in keywords:
            return "424420"
        elif "dairy" in keywords:
            return "424430"
        elif "seafood" in keywords:
            return "424460"
        elif "meat" in keywords:
            return "424470"
        elif "fruit" in keywords or "vegetable" in keywords:
            return "424480"
        elif "grocery" in keywords:
            return "424410"
        else:
            return "424490"
    else:
        for t in types:
            if t in GOOGLE_TYPE_TO_NAICS:
                return GOOGLE_TYPE_TO_NAICS[t]
    return ""

def parse_results(results, state, keyword, seen_place_ids):
    parsed = []
    for r in results:
        place_id = r.get('place_id')
        if not place_id or place_id in seen_place_ids:
            continue
        seen_place_ids.add(place_id)

        details = get_place_details(place_id)
        time.sleep(1)

        types = r.get('types', [])
        name = r.get('name', "")
        naics_code = get_naics_code_from_types(types, name)

        parsed.append({
            'State': state,
            'Category': keyword,
            'Business Name': name,
            'Address': details.get('formatted_address') or r.get('vicinity'),
            'Phone Number': details.get('formatted_phone_number'),
            'Website': details.get('website'),
            'Latitude': r['geometry']['location']['lat'],
            'Longitude': r['geometry']['location']['lng'],
            'Rating': r.get('rating'),
            'User Ratings': r.get('user_ratings_total'),
            'Place ID': place_id,
            'Types': ', '.join(types),
            'NAICS Code': naics_code
        })
    return parsed

def main():
    all_data = []
    seen_place_ids = set()

    for state, city in STATE_CITIES.items():
        print(f"🔍 Processing {state}...")
        lat, lng = get_coordinates(city)
        if not lat:
            print(f"❌ Could not find coordinates for {city}")
            continue

        for keyword in RETAIL_KEYWORDS:
            print(f"  → Searching: {keyword}")
            results = fetch_places(lat, lng, keyword)
            data = parse_results(results, state, keyword, seen_place_ids)
            all_data.extend(data)

    df = pd.DataFrame(all_data)
    df.to_csv("statewise_retailers_enriched_with_wholesale_naics.csv", index=False)
    print("✅ Data saved to statewise_retailers_enriched_with_wholesale_naics.csv")

if __name__ == "__main__":
    main()