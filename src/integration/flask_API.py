from flask import Flask
from flask_restful import Api, Resource, reqparse
import random
import pandas as pd
from pymongo import MongoClient
import certifi
import json

app = Flask(__name__)
api = Api(app)

# создадим пул штрихкодов по брендам+категориям, которые будем получать для каждого client_id


path_to_repo = '/Users/kuznetsovnikita'

with open(path_to_repo+'/recommendations/src/data/mongodb_pass.txt', 'r') as file:
    path2 = file.read()
client = MongoClient(path2, tlsCAFile=certifi.where())
current_db = client['spin_services']

# orders = current_db['cs_cart_orders'].find()

import_path = path_to_repo + '/recommendations/data/raw/'
vygruz = pd.read_excel(import_path+'goods.xlsx').iloc[:,1:]
vygruz  = vygruz.loc[vygruz.id.str.len() > 10]

# id_pool = vygruz.groupby(['brand','Группа категорий']).agg(
#     {'id':lambda x: list(x.astype(int))}).to_dict()['id']



@app.route('/')
def index():
    return "Hello, World сука!"

@app.route('/get_order/<int:id>')
def get(id):
    # order = current_db['сs_cart_orders'].find({'_id':int(id)})
    return json.dumps(list(current_db['сs_cart_orders'].find({'_id':int(id)},{'products':1}))[0])
        # ', '.join(vygruz.id.sample(n =5).to_list())
@app.route('/get_rec/<string:id>')
def get_rec(id):
    recs = ', '.join(current_db['recommends'].find({'_id':str(id)})[0]['recommends'])
    return recs

if __name__ == '__main__':
    app.run(debug=True)