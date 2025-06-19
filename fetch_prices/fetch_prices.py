import keepa
import pandas as pd
from datetime import datetime, timedelta, timezone
import json
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# ================================
# 🔧 CONFIGURATION PARAMETERS
# ================================
KEEPA_API_KEY = '32emg7srpcrnhllidjlmcjoqqklqm5jou6rciqhj1uav2t50cjmg5a7d81ffhm8i'
ASIN_JSON_PATH = '/Users/macm3/Desktop/Keepa_amazon/asins_fetch_via_scraping/asin_output_scraping/all_domains_top_asins.json'
DAYS = 180

# ================================
# 🧠 UTILITY FUNCTIONS
# ================================
def keepa_minutes_to_datetime(minutes):
    return datetime(2011, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=minutes)

def csv_to_daily_df(data, col_name):
    if not data:
        return pd.DataFrame(columns=['date', col_name])
    dates = [keepa_minutes_to_datetime(data[i]) for i in range(0, len(data), 2)]
    values = [data[i] / 100 if col_name != 'rating' else data[i] / 100 for i in range(1, len(data), 2)]
    df = pd.DataFrame({'date': dates, col_name: values})
    df['date'] = df['date'].dt.date
    df = df.groupby('date').first().reset_index()
    return df

def fetch_asin_data(api, asin, days, domain, category):
    try:
        product = api.query([asin], domain=domain, history=True)[0]
        csv = product.get('csv', [])
        retail = csv[3] if len(csv) > 3 and csv[3] else []
        discount = csv[0] if len(csv) > 0 and csv[0] else []
        rating = csv[16] if len(csv) > 16 and csv[16] else []

        df_retail = csv_to_daily_df(retail, 'retail_price')
        df_discount = csv_to_daily_df(discount, 'discounted_price')
        df_rating = csv_to_daily_df(rating, 'rating')

        df = pd.merge(df_retail, df_discount, on='date', how='outer')
        df = pd.merge(df, df_rating, on='date', how='outer')

        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=days - 1)
        full_range = pd.date_range(start=start_date, end=end_date, freq='D').date
        df_full = pd.DataFrame({'date': full_range})

        df = pd.merge(df_full, df, on='date', how='left').ffill().infer_objects(copy=False)

        df['asin'] = asin
        df['marketplace'] = domain.upper()
        df['category'] = category

        df = df.sort_values(by='date').reset_index(drop=True)  # ✅ Ensure sorted by date

        return df
    except Exception as e:
        print(f"[ERROR] ASIN {asin} in {domain.upper()}: {e}")
        return pd.DataFrame()

# ================================
# 🚀 MAIN EXECUTION
# ================================
if __name__ == "__main__":
    api = keepa.Keepa(KEEPA_API_KEY)

    with open(ASIN_JSON_PATH, 'r') as f:
        asin_data = json.load(f)

    all_data = pd.DataFrame()

    for domain_key, category_dict in asin_data.items():
        domain = domain_key.replace("Amazon", "").upper()

        for category, asin_list in category_dict.items():
            if not asin_list:
                continue

            for asin in asin_list:
                print(f"📦 Fetching: ASIN={asin}, Category={category}, Domain={domain}")
                df = fetch_asin_data(api, asin, DAYS, domain, category)
                if not df.empty:
                    all_data = pd.concat([all_data, df], ignore_index=True)

    print("\n📊 Final Combined Data Sample:")
    print(all_data.head())
    print(f"\n📊 Total Rows: {len(all_data)}")

    # ✅ Save with today's date as timestamp
    timestamp = datetime.now().strftime('%Y%m%d')
    filename = f"keepa_price_amazon_{timestamp}"

    all_data.to_csv(filename + '.csv', index=False)
    print(f"\n📁 Data saved to: {filename}.csv")