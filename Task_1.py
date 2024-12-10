import sqlite3
import msgpack
import pprint
import json

def load_msgpack(filename):
    with open(filename, 'rb') as f:
        loaded_data = msgpack.unpack(f)
    return loaded_data

def create_table(db):
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE books (
            title text UNIQUE,
            author text,
            genre text,
            pages integer,
            published_year integer,
            isbn text primary key,
            rating float,
            views integer
        )
    """)

def insert_data(db, loaded_data):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT OR IGNORE INTO books (title, author, genre, pages, published_year, isbn, rating, views)
        VALUES (:title, :author, :genre, :pages, :published_year, :isbn, :rating, :views)
    """, loaded_data)
    db.commit()

def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn

def first_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM books
        ORDER BY published_year
        LIMIT 96
        """
    )
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

def second_query(db):
    cursor = db.cursor()
    res = cursor.execute("SELECT COUNT(*) as count_books, "
                         "MIN(views) as min_views, "
                         "MAX(views) as max_views, "
                         "AVG(views) as average_views "
                         "FROM books")
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

def third_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            COUNT(*) as count,
            genre
        FROM books
        GROUP BY genre
    """)
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

def fourth_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT * FROM books
        WHERE rating > 4.5
        ORDER BY rating
        LIMIT 96
    """)
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

# #STEP 1
# create_table(connect_to_db("first_db"))
#STEP 2
data = load_msgpack(r"C:\Users\ipavl\Учеба_УРФУ\1_курс\1_семестр\ИД\Practice_4\86\1-2\item.msgpack")
db = connect_to_db("first_db")
insert_data(db, data)
# STEP 3
first_query_filt = first_query(db)
# print(second_query(db))
second_query_books = second_query(db)
third_query_books = third_query(db)
fourth_query_books = fourth_query(db)

# Сохраним вывод первых отсортированных строк в таблице в json
with open("first_task_filtered_data", "w", encoding = "utf-8") as f:
    json.dump(first_query_filt, f, ensure_ascii=False)

with open("first_task_stat", "w", encoding = "utf-8") as f:
    json.dump(second_query_books, f, ensure_ascii=False)

with open("first_task_freq_genre", "w", encoding = "utf-8") as f:
    json.dump(third_query_books, f, ensure_ascii=False)

with open("first_tasK_sorted", "w", encoding = "utf-8") as f:
    json.dump(fourth_query_books, f, ensure_ascii=False)

