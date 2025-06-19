import keepa
import pandas as pd
from datetime import datetime, timedelta

# # -------- CONFIG --------
# KEEPA_API_KEY = "32emg7srpcrnhllidjlmcjoqqklqm5jou6rciqhj1uav2t50cjmg5a7d81ffhm8i"  # Replace this with your actual Keepa key
# ASIN = "B01N1SE4EP"                   # Replace with your ASIN
# DOMAIN = "US"
# NUM_DAYS = 60

# # -------- INIT --------
# api = keepa.Keepa(KEEPA_API_KEY)

# response = api.query('B0088PUEPK')

# print(response[0].keys())

from datetime import datetime, timedelta, timezone
import pandas as pd
import keepa
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# ================================
# 🔧 CONFIGURATION PARAMETERS
# ================================
KEEPA_API_KEY = '32emg7srpcrnhllidjlmcjoqqklqm5jou6rciqhj1uav2t50cjmg5a7d81ffhm8i'          # Your Keepa API key
ASINS = ['B00GJ11LCU', 'B00WWCVAUI']              # ASIN list
DAYS = 180                                        # Days of history
DOMAIN = 'GB'                                     # 'US', 'JP', 'IN', etc.

# ================================
# 🧠 UTILITY FUNCTIONS
# ================================
def keepa_minutes_to_datetime(minutes):
    return datetime(2011, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=minutes)

def csv_to_daily_df(data, col_name):
    if not data:
        return pd.DataFrame(columns=['date', col_name])
    dates = [keepa_minutes_to_datetime(data[i]) for i in range(0, len(data), 2)]
    values = [data[i]/100 if col_name != 'rating' else data[i]/100 for i in range(1, len(data), 2)]
    df = pd.DataFrame({'date': dates, col_name: values})
    df['date'] = df['date'].dt.date
    df = df.groupby('date').first().reset_index()
    return df

def fetch_asin_data(api, asin, days, domain):
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
        return df
    except Exception as e:
        print(f"[ERROR] ASIN {asin}: {e}")
        return pd.DataFrame()

# ================================
# 🚀 MAIN EXECUTION
# ================================
if __name__ == "__main__":
    api = keepa.Keepa(KEEPA_API_KEY)

    all_data = pd.DataFrame()
    for asin in ASINS:
        print(f"📦 Fetching data for ASIN: {asin}")
        df = fetch_asin_data(api, asin, DAYS, DOMAIN)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)

    print("\n✅ Combined Data Sample:")
    print(all_data.head())
    print(f"\n📊 Total Rows: {len(all_data)} (Expected: {len(ASINS)} × {DAYS} = {len(ASINS) * DAYS})")

    # 💾 Save to CSV with Amazon domain in filename
    filename = f"keepa_price_amazon_{DOMAIN.lower()}"
    all_data.to_pickle(filename + '.pkl')
    all_data.to_csv(filename + '.csv', index=False)
    print(f"\n📁 Data saved to {filename}")



