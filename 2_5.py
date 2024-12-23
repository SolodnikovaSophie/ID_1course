# https://www.kaggle.com/datasets/ahmedshahriarsakib/usa-real-estate-dataset

import numpy as np
import pandas as pd
import json
import msgpack
import os

# Загрузим файл
df = pd.read_csv(r"../Practice_2/realtor-data.csv", sep = ",")
print(df.head(6))
print(f"Столбцы: {df.columns}")

#Выберем 7 колонок для анализа данных
df_selected = df[['street', 'city', 'state', 'price', 'bed', 'bath', 'acre_lot']]
print(df_selected.head(6))


df_price = df_selected.iloc[:, 3]
df_bed = df_selected.iloc[:, 4]
df_bath = df_selected.iloc[:, 5]
df_acre_lot = df_selected.iloc[:, 6]
df_city = df_selected.iloc[:, 1]
df_state = df_selected.iloc[:, 2]

city_frequencies = df_city.value_counts().to_dict()
state_frequencies = df_state.value_counts().to_dict()

analysis_results = {'price':
        {'max_price': df_price.max(),
        'min_price': df_price.min(),
        'mean_price': df_price.mean(),
        'sum_price': df_price.sum(),
        'std_price': df_price.std()
         },
'bed':
        {'max_bed': df_bed.max(),
        'min_bed': df_bed.min(),
        'mean_bed': df_bed.mean(),
        'sum_bed': df_bed.sum(),
        'std_bed': df_bed.std()
         },
'bath':
        {'max_bath': df_bath.max(),
        'min_bath': df_bath.min(),
        'mean_bath': df_bath.mean(),
        'sum_bath': df_bath.sum(),
        'std_bath': df_bath.std()
         },
'acre_lot':
        {'max_acre_lot': df_acre_lot.max(),
        'min_acre_lot': df_acre_lot.min(),
        'mean_acre_lot': df_acre_lot.mean(),
        'sum_acre_lot': df_acre_lot.sum(),
        'std_acre_lot': df_acre_lot.std()
         },
'city': city_frequencies,
'state': state_frequencies
}

# with open("fifth_task_analysis.json", "w", encoding = "utf-8") as f:
#     json.dump(analysis_results, f, ensure_ascii=False)
#
# df.to_json("realtor-data.json", orient='records', lines=True)
df.to_pickle("realtor-data.pickle")
df.to_json('realtor-data.json')
with open("realtor-data.msgpack", "wb") as outfile:
    packed = msgpack.packb(df.to_dict())
    outfile.write(packed)

print(f"csv        = {os.path.getsize('realtor-data.csv')}")
print(f"json       = {os.path.getsize('realtor-data.json')}")
print(f"msgpack    = {os.path.getsize('realtor-data.msgpack')}")
print(f"pickle     = {os.path.getsize('realtor-data.pickle')}")


