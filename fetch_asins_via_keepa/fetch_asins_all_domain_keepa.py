import pandas as pd
import keepa
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sentence_transformers import SentenceTransformer, util
from typing import Dict, List
import re

# ------------------- PARAMETERS -------------------
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1PPIC2TTRGap722HfZvFl9azMnJCoiNsORb8GDC1OyAQ/edit#gid=1089158624"
COLUMN_NAME = "Product Sub Category"

DOMAINS = {
    "AmazonJP": "JP"     # amazon.co.jp
}

NUM_ASINS = 1
KEEPA_API_KEY = "32emg7srpcrnhllidjlmcjoqqklqm5jou6rciqhj1uav2t50cjmg5a7d81ffhm8i"  # Replace this
OUTPUT_FILE = "asin_results_all_domains.json"

FALLBACK_KEYWORDS = [
    'hair care', 'skin care', 'fragrance', 'oral care', 'makeup', 'home care', 'food', 'beverages', 'alcohol',
    'fashion - apparel', 'fashion - footwear', 'fashion - accessories', 'electronics', 'health & supplements',
    'sports, fitness, & outdoors', 'toys, games, crafts', 'home', 'baby', 'pet', 'stationery, books, & supplies',
    'raw materials & commodities', 'industrial'
]
# --------------------------------------------------


def clean_keyword(keyword: str) -> str:
    """
    Clean keyword by removing common 'others'-style suffixes,
    dashes, underscores, extra spaces, etc.
    """
    keyword = keyword.lower().strip()
    
    # Remove suffixes like "-others", " others", "_others", "other", etc.
    keyword = re.sub(r'[\s\-_]*others?$', '', keyword)
    keyword = re.sub(r'[\s\-_]*other$', '', keyword)
    
    # Remove double spaces, dashes, underscores left behind
    keyword = re.sub(r'[-_]{2,}', '-', keyword)
    keyword = re.sub(r'\s{2,}', ' ', keyword)
    
    return keyword.strip()


# Initialize Keepa and SentenceTransformer
api = keepa.Keepa(KEEPA_API_KEY)
model = SentenceTransformer('all-MiniLM-L6-v2')
known_embeddings = model.encode(FALLBACK_KEYWORDS, convert_to_tensor=True)

def get_nearest_known_keyword(missing_keyword: str) -> str:
    missing_embedding = model.encode(missing_keyword, convert_to_tensor=True)
    similarities = util.cos_sim(missing_embedding, known_embeddings)[0]
    best_match_index = similarities.argmax().item()
    return FALLBACK_KEYWORDS[best_match_index]

# Load keywords from Google Sheet
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("your_google_service_account.json", scope)
client = gspread.authorize(creds)
spreadsheet = client.open_by_url(GOOGLE_SHEET_URL)
worksheet = spreadsheet.get_worksheet(0)
data = worksheet.get_all_records()
df = pd.DataFrame(data)
keywords = df[COLUMN_NAME].dropna().str.lower().unique()

# Output dictionary for all domains
result: Dict[str, Dict[str, List[str]]] = {}

# Main processing loop
for domain_name, domain_code in DOMAINS.items():
    result[domain_name] = {}
    print(f"\n=== Processing Domain: {domain_name} ({domain_code}) ===")

    for keyword in keywords:
        keyword = clean_keyword(keyword)
        asin_list = []
        fallback_keyword = None

        try:
            print(f"→ Searching keyword: '{keyword}' in {domain_name}")
            categories = api.search_for_categories(keyword, domain=domain_code)

            # Step 1: Try main keyword
            if not categories:
                print(f"   ❌ No category found. Fallback triggered.")
                fallback_keyword = get_nearest_known_keyword(keyword)
                print(f"   🔁 Using fallback keyword: '{fallback_keyword}'")
                categories = api.search_for_categories(fallback_keyword, domain=domain_code)

            # Step 2: Try best seller ASINs
            if categories:
                category_id = list(categories.keys())[0]
                response = api.best_sellers_query(category_id, domain=domain_code)
                if isinstance(response, list):
                    asin_list = response[:NUM_ASINS]

            # Step 3: If nothing from main keyword and fallback not yet tried
            if not asin_list and not fallback_keyword:
                fallback_keyword = get_nearest_known_keyword(keyword)
                print(f"   🔁 Empty result. Fallback to '{fallback_keyword}'")
                fallback_categories = api.search_for_categories(fallback_keyword, domain=domain_code)
                if fallback_categories:
                    fallback_cat_id = list(fallback_categories.keys())[0]
                    fallback_response = api.best_sellers_query(fallback_cat_id, domain=domain_code)
                    if isinstance(fallback_response, list):
                        asin_list = fallback_response[:NUM_ASINS]

        except Exception as e:
            print(f"   ⚠️ Error for '{keyword}' in {domain_name}: {e}")

        result[domain_name][keyword] = asin_list
        print(f"   ✅ ASINs fetched: {len(asin_list)}")

# Save combined result to JSON
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=4, ensure_ascii=False)

print(f"\n✅ JSON saved to {OUTPUT_FILE}")
