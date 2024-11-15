import csv

def read_csv(path):
    data = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append({
                'product_id': int(row['product_id']),
                'name': row['name'],
                'price': float(row['price']),
                'quantity': int(row['quantity']),
                'category': row['category'],
                'description': row['description'],
                'production_date': row['production_date'],
                #'expiration_date': row['expiration_date'],
                'rating': float(row['rating']),
                'status': row['status'],
            })
    return data

data = read_csv(r"C:\Users\ipavl\Учеба УРФУ\1 курс\1 семестр\ИД\86\fourth_task.txt")

size = len(data)
avg_quantity = 0
max_quantity = data[0]['quantity']
min_quantity = data[0]['quantity']

filtered_data = []

for item in data:
    avg_quantity += item['quantity']
    if max_quantity < item['quantity']:
        max_quantity = item['quantity']

    if min_quantity > item['quantity']:
        min_quantity = item['quantity']

    if item['category'] == 'Мясо':
        filtered_data.append(item)

avg_quantity /= size

with open("forth_task.txt", "w", encoding="utf-8") as f:
    f.write(f"Средняя цена: {avg_quantity}\n")
    f.write(f"Максимальная цена: {max_quantity}\n")
    f.write(f"Минимальная цена: {min_quantity}\n")

with open("forth_task.csv", "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, filtered_data[0].keys())
    writer.writeheader()
    for row in filtered_data:
        writer.writerow(row)
