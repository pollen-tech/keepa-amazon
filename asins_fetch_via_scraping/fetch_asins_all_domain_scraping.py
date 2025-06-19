import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sentence_transformers import SentenceTransformer, util
import json
import os
import re
import random
import time

# ---------------- CONFIGURATION ----------------

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
]

ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.8",
    "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
]

# DOMAIN_LABELS = {
#     "com" : "AmazonUS",
#     "co.uk" : "AmazonGB",
#     "co.jp": "AmazonJP",
#     "de": "AmazonDE"
# }

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1PPIC2TTRGap722HfZvFl9azMnJCoiNsORb8GDC1OyAQ/edit?pli=1#gid=1089158624"
COLUMN_NAME = "Product Sub Category"
CREDENTIALS_JSON = "your_google_service_account.json"

FALLBACK_KEYWORDS = [
    'hair care', 'skin care', 'fragrance', 'oral care', 'makeup', 'home care', 'food', 'beverages',
    'alcohol', 'fashion - apparel', 'fashion - footwear', 'fashion - accessories', 'electronics', 'health & supplments',
    'sports, fitness, & outdoors', 'toys, games, crafts', 'home', 'baby', 'pet', 'stationery, books, & supplies ',
    'raw materials & commodities', 'industrial'
]

# AMAZON_DOMAINS = ["com", "co.uk", "co.jp", "de"]

MAX_RESULTS = 50
OUTPUT_DIR = "asin_output_scraping"
MODEL_NAME = "all-MiniLM-L6-v2"

# ------------------------------------------------


def get_random_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": random.choice(ACCEPT_LANGUAGES)
    }


def clean_keyword(keyword: str) -> str:
    keyword = keyword.lower().strip()
    keyword = re.sub(r'[\s\-_]*others?$', '', keyword)
    keyword = re.sub(r'[\s\-_]*other$', '', keyword)
    keyword = re.sub(r'[-_]{2,}', '-', keyword)
    keyword = re.sub(r'\s{2,}', ' ', keyword)
    return keyword.strip()


def get_sheet_id_from_url(url):
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    return match.group(1) if match else None


def get_keywords_from_sheet(sheet_url, column_name):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_JSON, scope)
    client = gspread.authorize(creds)

    sheet_id = get_sheet_id_from_url(sheet_url)
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.sheet1

    headers = worksheet.row_values(1)
    col_index = headers.index(column_name) + 1
    col_values = worksheet.col_values(col_index)[1:]

    seen = set()
    unique_keywords = []
    for k in col_values:
        k_clean = clean_keyword(k)
        if k_clean and k_clean not in seen:
            seen.add(k_clean)
            unique_keywords.append(k_clean)
    return unique_keywords


def fetch_asins(keyword, max_results=50, amazon_domain='com'):
    asins = set()
    page = 1
    retry_attempts = 3

    while len(asins) < max_results and page <= 10:
        url = f"https://www.amazon.{amazon_domain}/s?k={requests.utils.quote(keyword)}&page={page}"
        headers = get_random_headers()

        success = False
        for attempt in range(retry_attempts):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                if "captcha" in response.text.lower() or response.status_code in [429, 503]:
                    print(f"🚫 Blocked or CAPTCHA on page {page} | Retrying ({attempt + 1}/{retry_attempts})...")
                    time.sleep(2 ** attempt + random.uniform(0.5, 1.5))
                    continue

                soup = BeautifulSoup(response.content, 'html.parser')
                results = soup.find_all('div', {'data-asin': True})

                for item in results:
                    asin = item.get('data-asin')
                    if asin and len(asin) == 10:
                        asins.add(asin)
                    if len(asins) >= max_results:
                        break

                if not results:
                    print(f"🟡 No results found on page {page}")
                    break

                success = True
                time.sleep(random.uniform(1.0, 2.5))
                break
            except Exception as e:
                print(f"❌ Error: {e} | Retrying ({attempt + 1}/{retry_attempts})...")
                time.sleep(2 ** attempt + random.uniform(0.5, 1.5))

        if not success:
            print(f"❗ Skipping page {page} after {retry_attempts} failed attempts.")
            break

        page += 1

    return list(asins)


def get_semantic_fallback(original_keyword, fallback_keywords, model):
    embedding = model.encode([original_keyword] + fallback_keywords, convert_to_tensor=True)
    sims = util.pytorch_cos_sim(embedding[0], embedding[1:])[0]
    best_index = sims.argmax().item()
    return fallback_keywords[best_index]


def fetch_with_fallbacks(keyword, all_keywords, fallback_keywords, model, domain, fallback_cache):
    asins = fetch_asins(keyword, MAX_RESULTS, domain)
    if len(asins) >= MAX_RESULTS:
        return asins

    total_asins = set(asins)
    print(f"⚠️ Only {len(total_asins)} ASINs found for '{keyword}' → trying fallback keywords...")

    tried_fallbacks = set()
    attempts = 0

    while attempts < 3 and len(total_asins) < MAX_RESULTS:
        best_fallback = get_semantic_fallback(keyword, [kw for kw in fallback_keywords if kw not in tried_fallbacks], model)
        if best_fallback in tried_fallbacks:
            break
        tried_fallbacks.add(best_fallback)

        if best_fallback in fallback_cache:
            print(f"♻️ Reusing cached ASINs for fallback: '{best_fallback}'")
            fallback_asins = fallback_cache[best_fallback]
        else:
            print(f"🔁 Fallback attempt {attempts+1}: '{best_fallback}'")
            fallback_asins = fetch_asins(best_fallback, MAX_RESULTS, domain)
            fallback_cache[best_fallback] = fallback_asins

        needed = MAX_RESULTS - len(total_asins)
        total_asins.update(fallback_asins[:needed])
        print(f"   ↳ After fallback '{best_fallback}', total ASINs: {len(total_asins)}")

        attempts += 1

    if len(total_asins) == 0:
        print(f"❌ No ASINs found for '{keyword}' after 3 fallbacks.")
        return []

    return list(total_asins)[:MAX_RESULTS]

def main():
    keywords = get_keywords_from_sheet(SPREADSHEET_URL, COLUMN_NAME)
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    model = SentenceTransformer(MODEL_NAME)
    all_results = {}

    for domain in AMAZON_DOMAINS:
        domain_results = {}
        fallback_cache = {}
        print(f"\n🌐 Domain: Amazon.{domain}\n" + "-" * 40)

        for keyword in keywords:
            print(f"🔍 Fetching for: '{keyword}'")
            asins = fetch_with_fallbacks(keyword, keywords, FALLBACK_KEYWORDS, model, domain, fallback_cache)
            domain_results[keyword] = asins
            print(f"✅ Fetched {len(asins)} ASINs for '{keyword}'\n")

        friendly_domain = DOMAIN_LABELS.get(domain, domain)
        all_results[friendly_domain] = domain_results

    combined_path = os.path.join(OUTPUT_DIR, "all_domains_top_asins.json")
    with open(combined_path, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"\n✅ All domain ASINs saved to: {combined_path}")

if __name__ == "__main__":
    main()