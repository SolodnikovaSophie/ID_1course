import msgpack
import pymongo
from mpmath import limit
from pymongo import MongoClient
import json
import pickle


def connect_db():
    client = MongoClient()
    db = client["db-2024"]
    print(db.jobs)
    return db.jobs

def load_msgpack(filename):
    with open(filename, 'rb') as f:
        loaded_data = msgpack.unpack(f)
    return loaded_data

def read_pkl(filename):
    with open(filename, "rb") as f:
        return pickle.load(f)

def read_json(filename):
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)

collection = connect_db()
# collection.insert_many(read_pkl("86/task_2_item.pkl"))
# collection.insert_many(load_msgpack("86/task_1_item.msgpack"))
# collection.insert_many(read_json("86/task_3_item.json"))

# 	вывод* первых 10 записей, отсортированных по убыванию по полю salary;
def sort_by_salary(collection):
    return list(collection.find(limit = 10).sort({"salary": pymongo.DESCENDING}))

# 	вывод первых 15 записей, отфильтрованных по предикату age < 30, отсортировать по убыванию по полю salary
def sort_by_age(collection):
    return list(collection.find({"age": {"$lt": 30}}).sort([("salary", pymongo.DESCENDING)]).limit(15))

# 	вывод первых 10 записей, отфильтрованных по сложному предикату: (записи только из произвольного города,
# записи только из трех произвольно взятых профессий), отсортировать по возрастанию по полю age
def sort_by_mix(collection):
    city_to_filter = "Санкт-Петербург"
    jobs_to_filter = ["Повар", "Водитель", "Оператор call-центра"]
    return list(collection.find({"$and": [
        {"city": {"$eq": city_to_filter}},
        {"job": {"$in": jobs_to_filter}}
    ]}).sort([("age", 1)]).limit(10))

# 	вывод количества записей, получаемых в результате следующей фильтрации (age в произвольном диапазоне,
# year в [2019,2022], 50 000 < salary <= 75 000 || 125 000 < salary < 150 000).
def len_of_sorted(collection):
    return collection.count_documents({
          "age": {"$gt": 20, "$lte": 70},
          "year": {"$gte": 2019, "$lte": 2022},
          "$or": [
            {"salary": {"$gt": 50000, "$lte": 75000}},
            {"salary": {"$gt": 125000, "$lt": 150000}}
          ]
        })

# 	вывод минимальной, средней, максимальной salary
def get_salary_agg(collection):
    q = [
        {
            "$group": {
                "_id": "result",
                "max_salary": {"$max": "$salary"},
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"}
            }
        }
    ]
    return list(collection.aggregate(q))

# 	вывод количества данных по представленным профессиям
def get_freq_by_job(collection):
    q = [
        {
            "$group": {
                "_id": "$job",
                "count": {"$sum": 1},
            }
        }
    ]
    return list(collection.aggregate(q))

# 	вывод минимальной, средней, максимальной salary по городу
def get_salary_city_agg(collection):
    q = [
        {
            "$group": {
                "_id": "$city",
                "max_salary": {"$max": "$salary"},
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"}
            }
        }
    ]
    return list(collection.aggregate(q))

# 	вывод минимальной, средней, максимальной salary по профессии
def get_salary_job_agg(collection):
    q = [
        {
            "$group": {
                "_id": "$job",
                "max_salary": {"$max": "$salary"},
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"}
            }
        }
    ]
    return list(collection.aggregate(q))

# 	вывод минимального, среднего, максимального возраста по городу
def get_age_city_agg(collection):
    q = [
        {
            "$group": {
                "_id": "$city",
                "max_age": {"$max": "$age"},
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"}
            }
        }
    ]
    return list(collection.aggregate(q))

# 	вывод минимального, среднего, максимального возраста по профессии
def get_age_job_agg(collection):
    q = [
        {
            "$group": {
                "_id": "$job",
                "max_age": {"$max": "$age"},
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"}
            }
        }
    ]
    return list(collection.aggregate(q))

# 	вывод максимальной заработной платы при минимальном возрасте
def get_max_salary_by_min_age(collection):
    q = [
        {
            "$group": {
                "_id": "$age",
                "max_salary": {"$max": "$salary"}
            }
        },
        {
            "$group": {
                "_id": "result",
                "min_age": {"$min": "$_id"},
                "max_salary": {"$max": "$max_salary"}
            }
        }
    ]
    return list(collection.aggregate(q))

# 	вывод минимальной заработной платы при максимальной возрасте
def get_min_salary_by_max_age(collection):
    q = [
        {
            "$group": {
                "_id": "$age",
                "min_salary": {"$max": "$salary"}
            }
        },
        {
            "$group": {
                "_id": "result",
                "max_age": {"$max": "$_id"},
                "min_salary": {"$min": "$min_salary"}
            }
        }
    ]
    return list(collection.aggregate(q))

# вывод минимального, среднего, максимального возраста по городу, при условии,
# что заработная плата больше 50 000, отсортировать вывод по убыванию по полю avg
def get_age_stat_by_city_with_match_and_sort(collection):
    q = [
        {
            "$match": {
                "salary": {"$gt": 50000}
            }
        },
        {
            "$group": {
                "_id": "$city",
                "max_age": {"$max": "$age"},
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"}
            }
        },
        {
            "$sort": {
                "avg_age": pymongo.DESCENDING
            }
        }
    ]
    return list(collection.aggregate(q))

# 	вывод минимальной, средней, максимальной salary
# в произвольно заданных диапазонах по городу, профессии,
# и возрасту: 18<age<25 & 50<age<65
def custom_query(collection):
    q = [
        {
            "$match": {
                "city": {"$in": ["Прага", "Мерида", "Варшава", "Аликанте"]},
                "job": {"$in": ["Повар", "Продавец", "Медсестра"]},
                "$or": [
                    {"age": {"$gt": 18, "$lt": 25}},
                    {"age": {"$gt": 50, "$lt": 65}},
                ]
            }
        },
        {
            "$group": {
                "_id": "$result",
                "max_age": {"$max": "$salary"},
                "min_age": {"$min": "$salary"},
                "avg_age": {"$avg": "$salary"}
            }
        }
    ]
    return list(collection.aggregate(q))

# произвольный запрос с $match, $group, $sort
def my_query(collection):
    q = [
        {
            "$match": {
                "year": {"$gt": 2010}
            }
        },
        {
            "$group": {
                "_id": "$job",
                "max_salary": {"$max": "$salary"},
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"}
            }
        },
        {
            "$sort": {
                "max_salary": pymongo.DESCENDING
            }
        }
    ]
    return list(collection.aggregate(q))

# 	удалить из коллекции документы по предикату: salary < 25 000 || salary > 175000
def delete_by_salary(collection):
    return collection.delete_many({
        "$or": [
            {"salary": {"$lt": 25000}},
            {"salary": {"$gt": 175000}},
        ]
    })

# 	увеличить возраст (age) всех документов на 1
def increase_age(collection):
    return collection.update_many({}, {
        "$inc": {
            "age": 1
        }
    })

# 	поднять заработную плату на 5% для произвольно выбранных профессий
def increase_salary(collection):
    return collection.update_many({
        "job": {"$in": ["Программист", "Инженер"]}
    }, {
        "$mul": {
            "salary": 1.05
        }
    })

# 	поднять заработную плату на 7% для произвольно выбранных городов
def increase_salary_for_city(collection):
    return collection.update_many({
        "city": {"$in": ["Сараево", "Осера", "Прага"]}
    }, {
        "$mul": {
            "salary": 1.07
        }
    })

# 	поднять заработную плату на 10% для выборки по сложному предикату
# (произвольный город, произвольный набор профессий, произвольный диапазон возраста)
def increase_salary_mix(collection):
    return collection.update_many({
        "city": {"$in": ["Сараево", "Осера", "Мерида"]},
        "job": {"$in": ["Повар", "Архитектор", "Водитель"]},
        "age": {"$gt": 20, "$lt": 57}
    }, {
        "$mul": {
            "salary": 1.1
        }
    })

# 	удалить из коллекции записи по произвольному предикату
def delete_by_salary(collection):
    return collection.delete_many({
        "$or": [
            {"age": {"$lt": 40}},
            {"age": {"$gt": 55}},
            {"job": {"$in": ["Оператор call-центра"]}}
        ]
    })

# print(delete_by_salary(collection))

sort_by_age_job = sort_by_age(collection)
sort_by_salary_job = sort_by_salary(collection)
sort_by_mix_job = sort_by_mix(collection)
len_of_sorted_job = len_of_sorted(collection)

# def write_to_json(data, filename):
#     with open(filename, "w", encoding="utf-8") as f:
#         json.dump(data, f, indent=4, default=str, ensure_ascii=False)
#
# write_to_json(sort_by_age(collection), "sort_by_age.json")
# write_to_json(sort_by_salary(collection), "sort_by_salary.json")
# write_to_json(sort_by_mix(collection), "sort_by_mix.json")
# write_to_json([len_of_sorted(collection)], "len_of_sorted.json")
# write_to_json(get_salary_agg(collection), "get_salary_agg.json")
# write_to_json(get_freq_by_job(collection), "get_freq_by_job.json")
# write_to_json(get_salary_city_agg(collection), "get_freq_by_job.json")
# write_to_json(get_salary_job_agg(collection), "get_salary_job_agg.json")
# write_to_json(get_age_city_agg(collection), "get_age_city_agg.json")
# write_to_json(get_age_job_agg(collection), "get_age_job_agg.json")
# write_to_json(get_max_salary_by_min_age(collection), "get_max_salary_by_min_age.json")
# write_to_json(get_min_salary_by_max_age(collection), "get_min_salary_by_max_age.json")
# write_to_json(get_age_stat_by_city_with_match_and_sort(collection), "get_age_stat_by_city_with_match_and_sort.json")
# write_to_json(custom_query(collection), "custom_query.json")
# write_to_json(my_query(collection), "my_query.json")
