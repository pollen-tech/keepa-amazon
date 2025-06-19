from keepa import Keepa
from sentence_transformers import SentenceTransformer, util
import pandas as pd
from deep_translator import GoogleTranslator  # pip install deep_translator
import langdetect
from internal_cat_and_sub_cat_map import internal_cat_sub_cat_map  # your internal (category, subcategory) list

# ----------- CONFIGURABLE PARAMETERS -------------
API_KEY = '32emg7srpcrnhllidjlmcjoqqklqm5jou6rciqhj1uav2t50cjmg5a7d81ffhm8i'
DOMAIN = 'US'  # Change to 'US', 'SG', etc. as needed
# -----------------------------------------------

# 1. Init Keepa API and embedding model
api = Keepa(API_KEY)
model = SentenceTransformer('all-MiniLM-L6-v2')

# 2. Get Keepa categories
print(f"📥 Fetching Keepa categories for domain: {DOMAIN}")
cats = api.category_lookup(0, domain=DOMAIN)

# 3. Traverse full category tree
def traverse_category_tree(cat_id, all_cats, parent_name='Root', parent_id=None):
    entries = []
    for cid, cat in all_cats.items():
        if cat.get('parent') == cat_id:
            subcat_name = cat.get("name", "")
            try:
                lang = langdetect.detect(subcat_name)
                if lang != 'en':
                    subcat_name_en = GoogleTranslator(source='auto', target='en').translate(subcat_name)
                else:
                    subcat_name_en = subcat_name
            except:
                subcat_name_en = subcat_name  # fallback

            entries.append({
                "cat_id": cat_id if cat_id else cid,
                "cat_name": parent_name,
                "subcat_id": cid,
                "subcat_name": subcat_name_en,
                "full_desc": f"{parent_name} - {subcat_name_en}"
            })

            # Recurse
            entries.extend(traverse_category_tree(cid, all_cats, parent_name=subcat_name_en, parent_id=cid))
    return entries

# 4. Build Keepa category-subcategory list
keepa_entries = traverse_category_tree(0, cats)
print(f"✅ Total processed Keepa categories: {len(keepa_entries)}")

# 5. Create embeddings
keepa_embeddings = model.encode([e['full_desc'] for e in keepa_entries], convert_to_tensor=True)

# 6. Match each internal (category, subcategory) to closest Keepa subcategory
results = []
for internal_cat, internal_subcat in internal_cat_sub_cat_map:
    query_desc = f"{internal_cat} - {internal_subcat}"
    query_emb = model.encode(query_desc, convert_to_tensor=True)
    scores = util.cos_sim(query_emb, keepa_embeddings)[0]
    idx = scores.argmax().item()
    best_match = keepa_entries[idx]
    
    results.append({
        f"Internal Cat_{DOMAIN}": internal_cat,
        f"Internal Subcat_{DOMAIN}": internal_subcat,
        f"Keepa Cat Name_{DOMAIN}": best_match["cat_name"],
        f"Keepa Cat ID_{DOMAIN}": best_match["cat_id"],
        f"Keepa Subcat Name_{DOMAIN}": best_match["subcat_name"],
        f"Keepa Subcat ID_{DOMAIN}": best_match["subcat_id"],
        f"Similarity_{DOMAIN}": round(scores[idx].item(), 3)
    })

# 7. Save to Excel
df = pd.DataFrame(results)
df.to_excel(f"mapped_internal_to_keepa_{DOMAIN}.xlsx", index=False)
print(f"💾 Mapping saved to: mapped_internal_to_keepa_{DOMAIN}.xlsx")
