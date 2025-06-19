import keepa
import pandas as pd
from datetime import datetime, timezone
import json
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

# ================================
# 🔧 CONFIGURATION
# ================================
# Load Keepa API key from secrets file
with open('../secrets/keepa_config.json') as f:
    keepa_config = json.load(f)

KEEPA_API_KEY = keepa_config["KEEPA_API_KEY"]
ASIN_JSON_PATH = '/Users/macm3/Desktop/Keepa_amazon/asins_fetch_via_scraping/asin_output_scraping/all_domains_top_asins.json'

# Target column schema
TARGET_COLUMNS = ["date", "retail_price", "discounted_price", "rating", "asin", "marketplace", "category"]

# ================================
# 🧠 Utility: Convert keepa time to datetime
# ================================
def keepa_minutes_to_datetime(minutes):
    return datetime(2011, 1, 1, tzinfo=timezone.utc) + pd.to_timedelta(minutes, unit='m')

# ================================
# 📦 Fetch today's prices
# ================================
def fetch_today_price(api, asin, domain, category):
    try:
        product = api.query([asin], domain=domain, history=True)[0]
        csv = product.get('csv', [])
        retail = csv[3] if len(csv) > 3 else []
        discount = csv[0] if len(csv) > 0 else []
        rating = csv[16] if len(csv) > 16 else []

        def extract_latest(data):
            if not data:
                return None
            dates = [keepa_minutes_to_datetime(data[i]).date() for i in range(0, len(data), 2)]
            values = [data[i+1] / 100 for i in range(0, len(data), 2)]
            df = pd.DataFrame({'date': dates, 'value': values})
            today = datetime.now().date()
            today_val = df[df['date'] == today]['value']
            return today_val.values[0] if not today_val.empty else None

        today = datetime.now().date()
        row = {
            'date': today,
            'retail_price': extract_latest(retail),
            'discounted_price': extract_latest(discount),
            'rating': extract_latest(rating),
            'asin': asin,
            'marketplace': domain.upper(),
            'category': category
        }

        if pd.isna(row['retail_price']) and pd.isna(row['discounted_price']):
            return None  # skip if no valid price

        return row

    except Exception as e:
        print(f"[ERROR] ASIN {asin} in {domain.upper()}: {e}")
        return None

# ================================
# 🚀 MAIN EXECUTION
# ================================
if __name__ == "__main__":
    api = keepa.Keepa(KEEPA_API_KEY)

    with open(ASIN_JSON_PATH, 'r') as f:
        asin_data = json.load(f)

    rows = []

    for domain_key, category_dict in asin_data.items():
        domain = domain_key.replace("Amazon", "").upper()
        print(f"🌍 Domain: {domain}")

        for category, asin_list in category_dict.items():
            if not asin_list:
                continue
            print(f"📂 Category: {category}")

            for asin in asin_list:
                print(f"🔍 Fetching ASIN: {asin}")
                record = fetch_today_price(api, asin, domain, category)
                if record:
                    rows.append(record)

    df_today = pd.DataFrame(rows)

    # Ensure correct column order and fill missing rating if needed
    for col in TARGET_COLUMNS:
        if col not in df_today.columns:
            df_today[col] = pd.NA
    df_today = df_today[TARGET_COLUMNS]

    print("\n✅ Sample of Today's Prices:")
    print(df_today.head())

    # Save result
    timestamp = datetime.now().strftime('%Y%m%d')
    filename = f"amazon_keepa_prices_{timestamp}.csv"
    df_today.to_csv(filename, index=False)
    print(f"\n💾 File saved to: {filename}")
