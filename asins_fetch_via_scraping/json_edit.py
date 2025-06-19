import json
import glob

# 🔹 Step 1: List all JSON files
json_files = ['/Users/macm3/Desktop/Keepa_amazon/asins_fetch_via_scraping/asin_output_scraping/asin_data_us_only.json',
                 '/Users/macm3/Desktop/Keepa_amazon/asins_fetch_via_scraping/asin_output_scraping/amazon_gb_only.json',
                 '/Users/macm3/Desktop/Keepa_amazon/asins_fetch_via_scraping/asin_output_scraping/all_domains_top_asins.json',]  # or use glob.glob("*.json")

# 🔹 Step 2: Merge all JSONs
merged_data = {}

for file in json_files:
    with open(file, 'r') as f:
        data = json.load(f)
        merged_data.update(data)  # ⚠️ Later keys will override earlier ones if duplicate

# 🔹 Step 3: Save the merged JSON
with open('merged_output.json', 'w') as f_out:
    json.dump(merged_data, f_out, indent=4)

print("✅ Merged JSON created: merged_output.json")