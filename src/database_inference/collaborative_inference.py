import pandas as pd
import numpy as np
import sys
sys.path.append(sys.path[1] +'/src/data')
from get_popular_items import *



def get_recommends_collection(user_dict, current_db, id_dict):
    "создает коллекцию монго формата из рекомендаций для каждого клида"
    recommends_collection = []
    for user in user_dict:
        user_recs = {}
        user_recs['_id'] = user
        recs_categ = user_dict[user]['recommends'].sort_values(ascending=False).head(5).index
        recs_id = [id_dict[categ] for categ in recs_categ]

        # собираем сортированный из чередующихся категорий
        recs_id_sorted = []
        max_len = max([len(l) for l in recs_id])
        for i in range(max_len):
            for list_ in recs_id:
                if i <= len(list_)-1:
                    recs_id_sorted.append(list_[i])

        user_recs['recommends'] = recs_id_sorted
        recommends_collection.append(user_recs)
    new_recs = list(get_popular_items(current_db))
    recommends_collection.append({'_id':'new','recommends': new_recs})
    return recommends_collection