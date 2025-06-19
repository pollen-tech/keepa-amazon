import pandas as pd
from datetime import datetime

# Automatically construct today’s filename
today_str = datetime.now().strftime('%Y%m%d')
csv_filename = f"amazon_keepa_prices_{today_str}.csv"

# Optional expected count file (set to path or None)
EXPECTED_COUNTS_CSV = None  # e.g., "expected_counts.csv"

# ================================
# 🧠 MONITORING SCRIPT
# ================================
def monitor_keepa_realtime_prices(csv_path, expected_path=None):
    df = pd.read_csv(csv_path, parse_dates=["date"])

    print("\n🔍 Basic Stats")
    print("-" * 30)
    print(f"📦 Total Rows Fetched: {len(df):,}")
    print(f"🧾 Unique ASINs Fetched: {df['asin'].nunique():,}")
    print(f"🌍 Unique Marketplaces: {sorted(df['marketplace'].unique())}")
    print(f"📂 Unique Categories: {sorted(df['category'].unique())}")

    print("\n📊 Breakdown by Marketplace")
    print("-" * 30)
    print(df.groupby("marketplace").size().to_frame("row_count"))

    print("\n📊 Breakdown by Marketplace + Category")
    print("-" * 30)
    print(df.groupby(["marketplace", "category"]).size().to_frame("row_count"))

    if expected_path:
        expected_df = pd.read_csv(expected_path)
        expected_df.columns = [col.lower() for col in expected_df.columns]

        actual_counts = df.groupby(["marketplace", "category"]).size().reset_index(name="actual_count")
        merged = pd.merge(expected_df, actual_counts, on=["marketplace", "category"], how="left")
        merged["actual_count"] = merged["actual_count"].fillna(0).astype(int)
        merged["success_pct"] = (merged["actual_count"] / merged["expected_count"]) * 100

        print("\n✅ Validation Against Expected Counts")
        print("-" * 30)
        print(merged)

        # Save summary
        summary_output = f"keepa_daily_validation_summary_{today_str}.csv"
        merged.to_csv(summary_output, index=False)
        print(f"\n📁 Validation summary saved to: {summary_output}")

# ================================
# ✅ Run It
# ================================
if __name__ == "__main__":
    monitor_keepa_realtime_prices(csv_filename, EXPECTED_COUNTS_CSV)
