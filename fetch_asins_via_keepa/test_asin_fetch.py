import keepa
key = '32emg7srpcrnhllidjlmcjoqqklqm5jou6rciqhj1uav2t50cjmg5a7d81ffhm8i'
api = keepa.Keepa(key)
categories = api.search_for_categories("shampoo", domain = 'DE')
# print(categories)

category = list(categories.items())[0][0]
# print(category)

asins = api.best_sellers_query(category, domain = 'DE')
print(asins)