import keepa
key = ''
api = keepa.Keepa(key)
categories = api.search_for_categories("shampoo", domain = 'DE')
# print(categories)

category = list(categories.items())[0][0]
# print(category)

asins = api.best_sellers_query(category, domain = 'DE')
print(asins)