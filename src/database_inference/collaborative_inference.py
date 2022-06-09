import pandas as pd
import numpy as np
import sys
sys.path.append(sys.path[1] +'/src/data')
from get_popular_items import *



def get_recommends_collection(user_dict, current_db, id_dict):
    "создает коллекцию монго формата из рекомендаций для каждого клида \
    user_dict: ключи - пользователи с персональными рекомендациями \
    id_dict: ключи - бренды-группы категорий, внутри соответствующие штрихкоды в стоке"
    recommends_collection = []
    vygruz = pd.DataFrame(current_db['vygruz_col'].find({'$and': [
        {'reason': 'Приемка'},
        {'Пол':
             {'$in':
                  ['male', 'female', 'Unisex']
              }
         }
    ]}))
    new_recs_male, new_recs_fem, new_recs = get_popular_items(current_db)
    for user in user_dict:
        user_recs = {}
        user_recs['_id'] = str(user)
        recs_categ = user_dict[user]['recommends'].sort_values(ascending=False).head(5).index
        recs_id = [id_dict.get(categ) for categ in recs_categ]

        # собираем сортированный из чередующихся категорий
        recs_id_sorted = []
        recs_id = [l if l != None else [] for l in recs_id]
        max_len = max([len(l) for l in recs_id])
        for i in range(max_len):
            for list_ in recs_id:
                if i <= len(list_) - 1:
                    recs_id_sorted.append(list_[i])

        male = list(vygruz.loc[(vygruz['reason'] == 'Приемка') & (vygruz['Пол'] != 'female')]['_id'].astype(str))
        female = list(vygruz.loc[(vygruz['reason'] == 'Приемка') & (vygruz['Пол'] != 'male')]['_id'].astype(str))

        # отбираем однополые рекомендации
        male_recs = [i for i in recs_id_sorted if i in male]
        male_recs.extend(new_recs_male)

        fem_recs = [i for i in recs_id_sorted if i in female]
        fem_recs.extend(new_recs_fem)

        user_recs['recommends'] = recs_id_sorted
        user_recs['recommends_male'] = male_recs
        user_recs['recommends_fem'] = fem_recs
        recommends_collection.append(user_recs)
    recommends_collection.append({
        '_id': 'new',
        'recommends': new_recs,
        'recommends_male': new_recs_male,
        'recommends_fem': new_recs_fem
    })
    return recommends_collection