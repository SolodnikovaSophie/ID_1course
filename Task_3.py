import csv
import sqlite3
import json

def read_csv(file):
    with open(file, mode ='r', encoding = "utf-8") as f:
      csvFile = csv.reader(f)
      for lines in csvFile:
            print(lines)
    return lines

def read_text(file):
    with open(file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        items = []
        item = {}
        for line in lines:
            if line == '=====\n':
                items.append(item)
                item = {}
                continue
            pair = line.strip().split("::")
            key = pair[0]
            if key in ['explicit', 'instrumentalness']: continue
            if key in ['duration_ms', 'year']:
                pair[1] = int(pair[1])
            if key in ['tempo', 'loudness']:
                pair[1] = float(pair[1])
            item[key] = pair[1]
    return items

def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn

def create_table(db):
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE artists (
            artist text,
            song text,
            duration_ms integer,
            year integer,
            tempo float,
            genre text,
            loudness float
        )
    """)

def insert_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO artists (artist, song, duration_ms, year, tempo, genre, loudness)
        VALUES (:artist, :song, :duration_ms, :year, :tempo, :genre, :loudness)
    """, data)
    db.commit()

def first_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM artists
        ORDER BY year
        LIMIT 96
        """
    )
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

def second_query(db):
    cursor = db.cursor()
    res = cursor.execute("SELECT COUNT(*) as count_artists, "
                         "MIN(tempo) as min_views, "
                         "MAX(tempo) as max_views, "
                         "AVG(tempo) as average_views "
                         "FROM artists")
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
        FROM artists
        GROUP BY genre
    """)
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

def fourth_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT * FROM artists
        WHERE year > 2010
        ORDER BY tempo
        LIMIT 96
    """)
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

data = read_text(r"C:\Users\ipavl\Учеба_УРФУ\1_курс\1_семестр\ИД\Practice_4\86\3\_part_2.text")
data_csv_f = read_csv(r"C:\Users\ipavl\Учеба_УРФУ\1_курс\1_семестр\ИД\Practice_4\86\3\_part_1.csv")

# STEP 1
# create_table(connect_to_db("third_db"))
# Insert into table
db = connect_to_db("third_db")
insert_data(db, data)
insert_data(db, data_csv_f)

first_query_filt = first_query(db)
second_query_artists = second_query(db)
third_query_artists = third_query(db)
fourth_query_artists = fourth_query(db)


with open("third_task_filtered_data", "w", encoding = "utf-8") as f:
    json.dump(first_query_filt, f, ensure_ascii=False)

with open("third_task_stat", "w", encoding = "utf-8") as f:
    json.dump(second_query_artists, f, ensure_ascii=False)

with open("third_task_freq_genre", "w", encoding = "utf-8") as f:
    json.dump(third_query_artists, f, ensure_ascii=False)

with open("third_task_sorted", "w", encoding = "utf-8") as f:
    json.dump(fourth_query_artists, f, ensure_ascii=False)