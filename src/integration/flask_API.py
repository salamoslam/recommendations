from flask import Flask
# from flask_restful import Api, Resource, reqparse
import random
import pandas as pd
from pymongo import MongoClient
import certifi
import json
import csv
import io
from pathlib import Path
import sys
from urllib.parse import unquote

application = Flask(__name__)
# api = Api(application)

# создадим пул штрихкодов по брендам+категориям, которые будем получать для каждого client_id

path = Path(sys.path[0])
path_to_repo = str(path.parent.parent.absolute())
with open(path_to_repo+'/src/data/mongodb_pass.txt', 'r') as file:
    path2 = file.read()
client = MongoClient(path2, tlsCAFile=certifi.where())
current_db = client['spin_services']


# recs_col = pd.DataFrame(current_db['recommends'].find())
# col_fil_ids = recs_col['_id'].to_list()


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


@application.route('/get_rec/<string:id>')
def get_rec(id):
    if id in col_fil_ids:
        recs = ', '.join(current_db['recommends'].find({'_id': str(id)})[0]['recommends'])
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
    application.run(debug=True)