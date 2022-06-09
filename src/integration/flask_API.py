from flask import Flask
# from flask_restful import Api, Resource, reqparse
import random
import pandas as pd
from pymongo import MongoClient
import certifi
import json
import csv
import io
from urllib.parse import unquote

application = Flask(__name__)
# api = Api(application)

# создадим пул штрихкодов по брендам+категориям, которые будем получать для каждого client_id


path_to_repo = '/Users/kuznetsovnikita'

path2 = 'mongodb://shmzl:1tAiGCElvXSHXex1@cluster0-shard-00-00.vs2je.mongodb.net:27017,cluster0-shard-00-01.vs2je.mongodb.net:27017,cluster0-shard-00-02.vs2je.mongodb.net:27017/test?authSource=admin&replicaSet=atlas-16vb3u-shard-0&readPreference=primary&ssl=true'
client = MongoClient(path2, tlsCAFile=certifi.where())
current_db = client['spin_services']


# orders = current_db['cs_cart_orders'].find()

# import_path = path_to_repo + '/recommendations/data/raw/'
# vygruz = pd.read_excel(import_path+'goods.xlsx').iloc[:,1:]
# vygruz  = vygruz.loc[vygruz.id.str.len() > 10]

# id_pool = vygruz.groupby(['brand','Группа категорий']).agg(
#     {'id':lambda x: list(x.astype(int))}).to_dict()['id']


@application.route('/')
def index():
    return "Hello, World!"


@application.route('/get_order/<int:id>')
def get(id):
    # order = current_db['сs_cart_orders'].find({'_id':int(id)})
    return json.dumps(list(current_db['сs_cart_orders'].find({'_id': int(id)}, {'products': 1}))[0])
    # ', '.join(vygruz.id.sample(n =5).to_list())


@application.route('/get_rec/<string:id>/<string:good_id>')
def get_rec(id, good_id):
    recs_col = pd.DataFrame(current_db['recommends'].find())
    gender =  pd.DataFrame(current_db['vygruz_col'].find({},{'Пол':1})).set_index('_id').to_dict()['Пол'].get(good_id)

    col_fil_ids = recs_col['_id'].to_list()
    #если не новый пользователь
    if id in col_fil_ids:
        # далее отбираем по полу просматриваемой шмотки
        if gender == 'male':
            recs = ', '.join(current_db['recommends'].find({'_id': str(id)})[0]['recommends_male'])
        elif gender == 'female':
            recs = ', '.join(current_db['recommends'].find({'_id': str(id)})[0]['recommends_fem'])
        else:
            recs = ', '.join(current_db['recommends'].find({'_id': str(id)})[0]['recommends'])
    # если новый пользователь
    else:
        if gender == 'male':
            recs = ', '.join(current_db['recommends'].find({'_id': 'new'})[0]['recommends_male'])
        elif gender == 'female':
            recs = ', '.join(current_db['recommends'].find({'_id': 'new'})[0]['recommends_fem'])
        else:
            recs = ', '.join(current_db['recommends'].find({'_id': 'new'})[0]['recommends'])
    return recs


@application.route('/luxxy_feed')
def get_feed():
    feed_array = list(current_db['feed_col'].find())
    new_feed_array = []
    for item in feed_array:
        ordered = list(item.keys())
        ordered[0] = 'id'
        item['id'] = item['_id']
        item.pop('_id')
        new_item = {k: item[k] for k in ordered}
        new_feed_array.append(new_item)
    data = new_feed_array  # your list of dicts

    with io.StringIO() as csvfile:
        fieldnames = new_feed_array[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in data:
            writer.writerow(row)
        string = csvfile.getvalue()
    return string


if __name__ == '__main__':
    application.run()