from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import certifi


# with open('mongodb_pass.txt', 'r') as file:
#     path2 = file.read()


def get_popular_items(current_db):
    heats = pd.DataFrame(current_db['cs_cart_heats'].find({'timestamp':
                                                               {'$gt': datetime.now() - timedelta(
                                                                   days=14)}}))
    prods = pd.DataFrame(current_db['cs_cart_products'].find())

    items = pd.DataFrame(current_db['vygruz_col'].find({'$and': [
        {'reason': 'Приемка'},
        {'Пол':
             {'$in':
                  ['male', 'female', 'Unisex']
              }
         }
    ]
    }))[['_id', 'Пол', 'brand', 'Группа категорий']].set_index('_id')

    heats.product_id = heats.product_id.astype(str).apply(lambda x: x[:-2])

    prod_rating = heats.loc[~heats.product_id.isin(['', '0'])].groupby('product_id').agg({'_id': 'count'}).sort_values(
        '_id', ascending=False)

    prod_rating = prod_rating.join(prods.loc[:, ['external_id', 'status']].set_index('external_id'))

    prod_rating = prod_rating.join(items).dropna(subset=['Пол'])

    male_rating = list(
        prod_rating.loc[(prod_rating.status == 'a') & (prod_rating['Пол'] != 'female')].sort_values('_id').tail(
            20).index)

    fem_rating = list(
        prod_rating.loc[(prod_rating.status == 'a') & (prod_rating['Пол'] != 'male')].sort_values('_id').tail(20).index)

    full_rating = list(
        prod_rating.loc[(prod_rating.status == 'a')].sort_values('_id').tail(
            20).index)
    male_rating.reverse()
    fem_rating.reverse()
    full_rating.reverse()

    return male_rating, fem_rating, full_rating