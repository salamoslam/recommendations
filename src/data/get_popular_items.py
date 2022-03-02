from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import certifi


# with open('mongodb_pass.txt', 'r') as file:
#     path2 = file.read()


def get_popular_items(current_db):

    heats = pd.DataFrame(current_db['cs_cart_heats'].find({'timestamp':
                                                           {'$gt':datetime.now()-timedelta(days=14)}}))
    prods = pd.DataFrame(current_db['cs_cart_products'].find())

    heats.product_id = heats.product_id.astype(str).str.rstrip('.0')

    prod_rating = heats.loc[heats.product_id != ''].groupby('product_id').agg({'_id':'count'}).sort_values('_id', ascending = False)

    prod_rating = prod_rating.join(prods.loc[:,['external_id','status']].set_index('external_id'))

    return list(prod_rating.loc[prod_rating.status == 'a'].sort_values('_id').tail(20).index)