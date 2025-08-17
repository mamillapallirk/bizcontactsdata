# === OSM / Open Data Config ===
LOCATION = "Shelton, CT"
RADIUS_MILES = 25  # used in filenames; converted to meters in code

# Be polite & compliant with Nominatim (add a real contact email)
NOMINATIM_USER_AGENT = "b2bees-us-scan/1.0 (contact: you@example.com)"
NOMINATIM_EMAIL = "you@example.com"

# Overpass API endpoint (use a mirror if needed)
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# ---------------------------------------------------------------------
# RETAIL: grocery/supermarket, dairy, flowers, personal care,
# department/general stores, fruit/veg, alcohol
# ---------------------------------------------------------------------
RETAIL_OSM_TAGS = [
    # Grocery & supermarkets
    ("shop", "supermarket"),
    ("shop", "grocery"),
    ("shop", "convenience"),

    # Fruits & vegetables
    ("shop", "greengrocer"),

    # Specialty foods
    ("shop", "organic"),
    ("shop", "dairy"),
    ("shop", "cheese"),
    ("shop", "chocolate"),
    ("shop", "confectionery"),
    ("shop", "butcher"),

    # Flowers
    ("shop", "florist"),

    # Personal care
    ("shop", "cosmetics"),
    ("shop", "perfumery"),
    ("shop", "beauty"),
    ("shop", "chemist"),
    ("shop", "health_food"),

    # Department / general
    ("shop", "department_store"),
    ("shop", "general"),
    ("shop", "variety_store"),

    # Alcohol-focused retail
    ("shop", "alcohol"),
    ("shop", "beverages"),
    ("shop", "wine"),
    ("shop", "beer"),
    ("shop", "tobacco"),
]

# ---------------------------------------------------------------------
# WHOLESALE / SUPPLIERS:
# OSM commonly uses shop=wholesale plus a subtag 'wholesale=*'
# Search both explicit shop tag and wholesale subtag values.
# ---------------------------------------------------------------------
WHOLESALE_OSM_TAGS = [
    ("shop", "wholesale"),

    # Food & beverage wholesale specializations
    ("wholesale", "food"),
    ("wholesale", "groceries"),
    ("wholesale", "beverages"),
    ("wholesale", "alcohol"),
    ("wholesale", "wine"),
    ("wholesale", "beer"),
    ("wholesale", "fruit"),
    ("wholesale", "vegetables"),
    ("wholesale", "meat"),
    ("wholesale", "seafood"),
    ("wholesale", "dairy"),

    # Personal care & household supplies
    ("wholesale", "cosmetics"),
    ("wholesale", "health_products"),
    ("wholesale", "chemicals"),
    # ("industrial", "warehouse"),  # optional/noisy
]

# ---------------------------------------------------------------------
# NAICS mapping for RETAIL tags (approximate 2017/2022)
# ---------------------------------------------------------------------
OSM_TO_NAICS_RETAIL = {
    ("shop", "supermarket"): "445110",
    ("shop", "grocery"): "445110",
    ("shop", "convenience"): "445120",
    ("shop", "greengrocer"): "445230",
    ("shop", "organic"): "445110",
    ("shop", "dairy"): "445299",
    ("shop", "cheese"): "445299",
    ("shop", "chocolate"): "445292",
    ("shop", "confectionery"): "445292",
    ("shop", "butcher"): "445210",
    ("shop", "florist"): "453110",
    ("shop", "cosmetics"): "446120",
    ("shop", "perfumery"): "446120",
    ("shop", "beauty"): "446120",
    ("shop", "chemist"): "446110",
    ("shop", "health_food"): "446191",
    ("shop", "department_store"): "452210",
    ("shop", "general"): "452319",
    ("shop", "variety_store"): "452319",
    ("shop", "alcohol"): "445310",
    ("shop", "beverages"): "445310",
    ("shop", "wine"): "445310",
    ("shop", "beer"): "445310",
    ("shop", "tobacco"): "453991",
}

# ---------------------------------------------------------------------
# Wholesale NAICS inference (keywords -> 42xxxx)
# ---------------------------------------------------------------------
WHOLESALE_KEYWORD_TO_NAICS = [
    ("frozen", "424420"),
    ("dairy", "424430"),
    ("seafood", "424460"),
    ("fish", "424460"),
    ("meat", "424470"),
    ("poultry", "424470"),
    ("fruit", "424480"),
    ("vegetable", "424480"),
    ("produce", "424480"),
    ("grocery", "424410"),
    ("general line", "424410"),
    ("food service", "424410"),
    ("beverage", "424490"),
    ("alcohol", "424820"),
    ("wine", "424820"),
    ("beer", "424810"),
    ("cosmetic", "424210"),
    ("health", "424210"),
    ("chemical", "424690"),
]
DEFAULT_WHOLESALE_NAICS = "424490"
