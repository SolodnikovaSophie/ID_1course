import pickle
import json

with open(r"C:\Users\ipavl\Учеба УРФУ\1 курс\1 семестр\ИД\Practice_2\86\fourth_task_products.json", "rb") as f:
    products = pickle.load(f)

with open(r"C:\Users\ipavl\Учеба УРФУ\1 курс\1 семестр\ИД\Practice_2\86\fourth_task_updates.json", "r", encoding="utf-8") as f:
    updates = json.load(f)

product_map = {}

for product in products:
    product_map[product['name']] = product

for update in updates:
    product = product_map[update['name']]

methods = {
    'percent-': lambda price, param: price * (1 - param),
    'percent+': lambda price, param: price * (1 + param),
    'sub': lambda price, param: price - param,
    'add': lambda price, param: price + param,
}

for update in updates:
    product = product_map[update['name']]
    product['price'] = methods[update['method']](product['price'], update['param'])

products = list(product_map.values())
with open("fourth_task.pkl", "wb") as f:
    pickle.dump(products, f)

