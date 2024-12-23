import pymongo
from pymongo import MongoClient
import pandas as pd
import json

def connect_db():
    client = MongoClient()
    db = client["db-2024"]
    print(db.students)
    return db.students

df = pd.read_csv('../task 4/student_lifestyle_dataset.csv', sep=',')
data = df.to_dict(orient='records')

# Проверить на пропуски
null_values = df.isnull().sum()
# print(null_values)
# print(data)

collection = connect_db()
# collection.insert_many(data)
# Подключение к MongoDB

collection = connect_db()

# Запросы на выборку

# Вывести студентов, которые спят в сутки меньше 5 часов
def sort_by_sleep_time(collection):
    """Выводит студентов, спящих меньше 5 часов, отсортированных по времени сна (по возрастанию)."""
    students = list(collection.find({"Sleep_Hours_Per_Day": {"$lt" : 5.5}}).sort([("Sleep_Hours_Per_Day", pymongo.ASCENDING)]))
    return students

# вывести всех студентов с уровнем стресса high и отсортировать по убыванию
def stress_students_level(collection):
    return list(collection.find({"Stress_Level": "High"}).sort([("Sleep_Hours_Per_Day", pymongo.ASCENDING)]).limit(100))

# вывести всех студентов, которые учатся от 6 до 9 часов в день. Отсортировать по количеству часов обучения в день
def study_hours(collection):
    return list(collection.find({"Study_Hours_Per_Day": {"$gt": 6, "$lt": 9}}).sort([("Study_Hours_Per_Day", pymongo.ASCENDING)]).limit(100))

# вывести всех студентов, которые общаются больше 3 часов в день. Отсортировать по количеству часов обучения в день
def students_social_activity(collection):
    return list(collection.find({"Social_Hours_Per_Day": {"$gt": 3}}).sort([("Social_Hours_Per_Day", pymongo.DESCENDING)]).limit(100))

# вывести всех студентов у которых gpa больше  или равно 3. Отсортировать по количеству часов обучения в день
def mix_query(collection):
    return list(collection.find({"$and": [
        {"GPA": {"$gte": 3}},
        {"Stress_Level": "Low"},
        {"Sleep_Hours_Per_Day": {"$gt": 8}}
    ]}))

# Произвести запросы на выборку с агрегацией
# Вывод минимального, среднего и максимального количества сна
def get_sleep_hours_agg(collection):
    q = [
        {
            "$group": {
                "_id": "result",
                "max_hours_of_sleep_time": {"$max": "$Sleep_Hours_Per_Day"},
                "min_hours_of_sleep_time": {"$min": "$Sleep_Hours_Per_Day"},
                "avg_hours_of_sleep_time": {"$avg": "$Sleep_Hours_Per_Day"}
            }
        }
    ]
    return list(collection.aggregate(q))

# 	вывод минимального, среднего, максимального количества часов сна по признаку stress_level
def get_sleep_hours_stress_level_agg(collection):
    q = [
        {
            "$group": {
                "_id": "$Stress_Level",
                "max_sleep_hours": {"$max": "$Sleep_Hours_Per_Day"},
                "min_sleep_hours": {"$min": "$Sleep_Hours_Per_Day"},
                "avg_sleep_hours": {"$avg": "$Sleep_Hours_Per_Day"}
            }
        }
    ]
    return list(collection.aggregate(q))

# 	вывод минимального, среднего, максимального возраста по уровню физической активности
def get_Physical_Activity_Stress_agg(collection):
    q = [
        {
            "$group": {
                "_id": "$Stress_Level",
                "max_Physical_Activity": {"$max": "$Physical_Activity_Hours_Per_Day"},
                "min_Physical_Activity": {"$min": "$Physical_Activity_Hours_Per_Day"},
                "avg_Physical_Activity": {"$avg": "$Physical_Activity_Hours_Per_Day"}
            }
        }
    ]
    return list(collection.aggregate(q))

# Максимальный GPA при минимальном количестве часов занятий
def get_max_GPA_by_min_Hours(collection):
    q = [
        {
            "$group": {
                "_id": "$GPA",
                "min_Study_Hours": {"$min": "$Study_Hours_Per_Day"}
            }
        },
        {
            "$group": {
                "_id": "result",
                "GPA": {"$max": "$_id"},
                "min_Study_Hours": {"$min": "$min_Study_Hours"}
            }
        }
    ]
    return list(collection.aggregate(q))

# 	Минимальный GPA при максимальном количестве часов занятий
def get_min_GPA_by_max_Hours(collection):
    q = [
        {
            "$group": {
                "_id": "$GPA",
                "max_Study_Hours": {"$max": "$max_Study_Hours"}
            }
        },
        {
            "$group": {
                "_id": "result",
                "GPA": {"$min": "$_id"},
                "max_Study_Hours": {"$max": "$max_Study_Hours"}
            }
        }
    ]
    return list(collection.aggregate(q))


sort_by_sleep_time_res = sort_by_sleep_time(collection)
stress = stress_students_level(collection)
study = study_hours(collection)
social_activity = students_social_activity(collection)
mix = mix_query(collection)
agg_sleep = get_sleep_hours_agg(collection)
agg_sleep_hours_stress_level = get_sleep_hours_stress_level_agg(collection)
agg_get_Physical_Activity = get_Physical_Activity_Stress_agg(collection)
agg_max_GPA_by_min_Hours = get_max_GPA_by_min_Hours(collection)
agg_min_GPA_by_max_Hours = get_min_GPA_by_max_Hours(collection)

def write_to_json(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, default=str, ensure_ascii=False)

write_to_json(sort_by_sleep_time_res, "sort_by_sleep_time_res.json")
write_to_json(stress, "stress_students_level.json")
write_to_json(study, "study_hours.json")
write_to_json(social_activity, "students_social_activity.json")
write_to_json(mix, "mix_query.json")
write_to_json(agg_sleep, "get_sleep_hours_agg.json")
write_to_json(agg_sleep_hours_stress_level, "agg_sleep_hours_stress_level.json")
write_to_json(agg_get_Physical_Activity, "agg_get_Physical_Activity.json")
write_to_json(agg_max_GPA_by_min_Hours, "agg_max_GPA_by_min_Hours.json")
write_to_json(agg_min_GPA_by_max_Hours, "agg_min_GPA_by_max_Hours.json")




