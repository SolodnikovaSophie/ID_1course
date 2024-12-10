from csv import DictReader

import msgpack
import sqlite3
import json
import csv


def process_csv(filename):
    with open(filename, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter=';', fieldnames=["name", "method", "param"])
        data = []
        for row in reader:
            row_str = "".join(row.values())
            if row_str.strip():
                try:
                    processed_row = {
                        "name": row["name"].strip(),
                        "method": row["method"].strip(),
                        "param": None
                    }
                    if row["method"] == "available":
                        processed_row["param"] = row["param"].strip().lower() == "true"
                    elif row["method"] in ("price_percent", "price_abs"):
                        processed_row["param"] = float(row["param"].strip())
                    elif row["method"] in ("quantity_sub", "quantity_add"):
                        processed_row["param"] = int(row["param"].strip())
                    data.append(processed_row)
                except (KeyError, ValueError) as e:
                    print(f"Ошибка обработки строки: {row}, Ошибка: {e}")
    return data

def load_msgpack(filename):
    key = ['category', 'fromCity', 'isAvailable', 'name', 'price', 'quantity', 'views']
    with open(filename, 'rb') as f:
        loaded_data = msgpack.unpack(f)
        items = []
        for item in loaded_data:
            try:
                cleaned_item = {
                    'name': item['name'],
                    'price': float(item['price']),
                    'quantity': int(item['quantity']),
                    'category': item.get('category', 'no'),  # значение по умолчанию
                    'fromCity': item['fromCity'],
                    'isAvailable': int(item['isAvailable']), #преобразуем True/False в 1/0
                    'views': int(item['views'])
                }
                items.append(cleaned_item)
            except (KeyError, ValueError, TypeError) as e:
                print(f"Ошибка обработки строки: {item}, ошибка: {e}")
    return items

def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn

def create_table(db):
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE product (
            name text,
            price float,
            quantity integer,
            category text,
            fromCity text,
            isAvailable integer,
            views integer,
            version integer default 0
        )
    """)

def insert_data(db, items):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO product (name, price, quantity, category, fromCity, isAvailable, views)
        VALUES (:name, :price, :quantity, :category, :fromCity, :isAvailable, :views)
    """, items)
                       # [{**item, 'category': item.get('category', '')} for item in items])
    db.commit()

def handle_remove(db, name):
    cursor = db.cursor()
    cursor.execute("DELETE FROM product WHERE name = ?", [name])
    db.commit()

def handle_price_percent(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        UPDATE product 
        SET price = ROUND(price * (1 + ?), 2), 
            version = version + 1 
        WHERE name =?""",
        [param, name]
    )
    db.commit()

def handle_price_abs(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        UPDATE product 
        SET price = price + ?, 
            version = version + 1 
        WHERE name =?""",
        [param, name]
    )
    db.commit()

def handle_quantity(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        UPDATE product 
        SET quantity = quantity + ?, 
            version = version + 1 
        WHERE name =?""",
        [param, name]
    )
    db.commit()

def handle_available(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        UPDATE product 
        SET isAvailable = ?, 
            version = version + 1 
        WHERE name =?""",
        [param, name]
    )
    db.commit()

def handle_updates(db, updates):
    # {'method', 'price_abs', 'quantity_sub', 'available', 'quantity_add', 'price_percent', 'remove'}
    for update in updates:
        if update['method'] == 'remove':
            handle_remove(db, update['name'])
        elif update['method'] == 'price_percent':
            handle_price_percent(db, update['name'], update['param'])
        elif update['method'] == 'price_abs':
            handle_price_abs(db, update['name'], update['param'])
        elif update['method'] == 'quantity_add':
            handle_quantity(db, update['name'], update['param'])
        elif update['method'] == 'quantity_sub':
            handle_quantity(db, update['name'], update['param'])
        elif update['method'] == 'available':
            handle_available(db, update['name'], update['param'])

def first_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT * 
        FROM product
        ORDER BY version 
        DESC LIMIT 10
        """
    )
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

def second_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
        category,
        SUM(price) as sum_price,
        MIN(price) as min_price,
        MAX(price) as max_price,
        AVG(price) as average_price,
        COUNT(*) as num_product
        FROM product
        GROUP BY category
        ORDER BY category
        """)
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

def third_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
        category,
        SUM(quantity) as sum_quantity,
        MIN(quantity) as min_quantity,
        MAX(quantity) as max_quantity,
        AVG(quantity) as average_quantity,
        COUNT(*) as num_product
        FROM product
        GROUP BY category
        ORDER BY category
        """)
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

def fourth_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT * FROM product
        WHERE price > 5000
        ORDER BY views
    """)
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

#STEP1
filename = r"C:\Users\ipavl\Учеба_УРФУ\1_курс\1_семестр\ИД\Practice_4\86\4\_update_data.csv"
data = "86/4/_product_data.msgpack"
updates = process_csv(filename)
items = load_msgpack(data)

# create_table(connect_to_db("fourth_db"))
#STEP 2
db = connect_to_db("fourth_db")
insert_data(db, items)
handle_updates(db, updates)

first_query_top = first_query(db)
second_query_product = second_query(db)
third_query_product = third_query(db)
fourth_query_product = fourth_query(db)

with open("fourth_task_filtered_data", "w", encoding = "utf-8") as f:
    json.dump(first_query_top, f, ensure_ascii=False)

with open("fourth_task_price", "w", encoding = "utf-8") as f:
    json.dump(second_query_product, f, ensure_ascii=False)

with open("fourth_task_quantity", "w", encoding = "utf-8") as f:
    json.dump(third_query_product, f, ensure_ascii=False)

with open("fourth_task_sorted", "w", encoding = "utf-8") as f:
    json.dump(fourth_query_product, f, ensure_ascii=False)