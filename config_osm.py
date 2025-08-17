# === OSM / Open Data Config ===
LOCATION = "Shelton, CT"
RADIUS_MILES = 25  # used in filenames; converted to meters in code

# Be polite & compliant with Nominatim (add a real contact email)
NOMINATIM_USER_AGENT = "b2bees-us-scan/1.0 (contact: you@example.com)"
NOMINATIM_EMAIL = "you@example.com"

# Overpass API endpoint (use a mirror if needed)
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# ---------------------------------------------------------------------
# RETAIL: tightly focused on grocery/supermarket, dairy, flowers,
# personal care, department/general stores, fruits/vegetables, alcohol
# ---------------------------------------------------------------------
# These are (key, value) pairs exactly as OSM tags are stored.
RETAIL_OSM_TAGS = [
    # Grocery & supermarket core
    ("shop", "supermarket"),
    ("shop", "grocery"),
    ("shop", "convenience"),

    # Fruits & vegetables
    ("shop", "greengrocer"),   # fruit & veg markets

    # Specialty foods (closest NAICS mapped below)
    ("shop", "organic"),
    ("shop", "dairy"),
    ("shop", "cheese"),
    ("shop", "chocolate"),
    ("shop", "confectionery"),
    ("shop", "butcher"),       # meat markets

    # Flowers
    ("shop", "florist"),

    # Personal care
    ("shop", "cosmetics"),
    ("shop", "perfumery"),
    ("shop", "beauty"),
    ("shop", "chemist"),       # drugstore (UK tagging)
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
# We search both the explicit shop tag and the wholesale subtag values.
# ---------------------------------------------------------------------
WHOLESALE_OSM_TAGS = [
    # Generic wholesale
    ("shop", "wholesale"),

    # Food & beverage wholesale specializations (via 'wholesale=*' subtag)
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

    # Distribution hubs (optional; can be noisy)
    # ("industrial", "warehouse"),
]

# ---------------------------------------------------------------------
# NAICS mapping for RETAIL tags
# (Most commonly used U.S. NAICS 2017/2022 approximations)
# ---------------------------------------------------------------------
OSM_TO_NAICS_RETAIL = {
    ("shop", "supermarket"): "445110",         # Supermarkets and Other Grocery (except Convenience) Stores
    ("shop", "grocery"): "445110",
    ("shop", "convenience"): "445120",         # Convenience Stores

    ("shop", "greengrocer"): "445230",         # Fruit and Vegetable Markets

    ("shop", "organic"): "445110",             # Often grocery-style organic stores
    ("shop", "dairy"): "445299",               # All Other Specialty Food Stores
    ("shop", "cheese"): "445299",
    ("shop", "chocolate"): "445292",           # Confectionery and Nut Stores
    ("shop", "confectionery"): "445292",
    ("shop", "butcher"): "445210",             # Meat Markets

    ("shop", "florist"): "453110",             # Florists

    ("shop", "cosmetics"): "446120",           # Cosmetics, Beauty Supplies, and Perfume Stores
    ("shop", "perfumery"): "446120",
    ("shop", "beauty"): "446120",
    ("shop", "chemist"): "446110",             # Pharmacies and Drug Stores
    ("shop", "health_food"): "446191",         # Food (Health) Supplement Stores

    ("shop", "department_store"): "452210",    # Department Stores
    ("shop", "general"): "452319",             # All Other General Merchandise Stores
    ("shop", "variety_store"): "452319",       # Dollar/variety stores

    ("shop", "alcohol"): "445310",             # Beer, Wine, and Liquor Stores
    ("shop", "beverages"): "445310",
    ("shop", "wine"): "445310",
    ("shop", "beer"): "445310",
    ("shop", "tobacco"): "453991",             # Tobacco Stores (older NAICS; acceptable)
}

# ---------------------------------------------------------------------
# Wholesale NAICS inference:
# We infer 42xxxx codes from keywords (name/tags blob) when wholesale is detected
# ---------------------------------------------------------------------
WHOLESALE_KEYWORD_TO_NAICS = [
    ("frozen", "424420"),      # Packaged Frozen Food
    ("dairy", "424430"),       # Dairy Product
    ("seafood", "424460"),     # Fish and Seafood
    ("fish", "424460"),
    ("meat", "424470"),        # Meat and Meat Product
    ("poultry", "424470"),
    ("fruit", "424480"),       # Fresh Fruit and Vegetable
    ("vegetable", "424480"),
    ("produce", "424480"),
    ("grocery", "424410"),     # General Line Grocery
    ("general line", "424410"),
    ("food service", "424410"),
    ("beverage", "424490"),    # fallback for beverages unless alcohol explicitly matched above
    ("alcohol", "424820"),     # Wine & Distilled Alcoholic Beverage (if you want finer granularity)
    ("wine", "424820"),
    ("beer", "424810"),        # Beer and Ale Merchant Wholesalers
    ("cosmetic", "424210"),    # Drugs and Druggists' Sundries (approx for personal care wholesale)
    ("health", "424210"),
    ("chemical", "424690"),    # Other Chemical and Allied Products
]
# A safe default if none of the keywords matched but wholesale is certain:
DEFAULT_WHOLESALE_NAICS = "424490"            # Other Grocery and Related Products
