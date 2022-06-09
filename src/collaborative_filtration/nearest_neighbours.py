import sys
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.insert(1, '../src/data')

# from get_users_info import *
# from get_brand_category_info import *
# from get_preference_matrix import *
import implicit
import faiss
from tqdm import tqdm
from scipy.sparse import csr_matrix
from scipy.spatial.distance import euclidean, cosine
from sklearn.metrics import precision_score
from itertools import islice
from pymongo import MongoClient
import certifi
import numpy as np
import pandas as pd

def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))

def recommend_NN(user_item_cut, user_item_cut_index, metric='euclid', k=10, method='faiss', inference = False):
    'берет на вход датафрейм с накликавшими пользователями (cf-able), каждому из них ищет k соседей\
     на основе metric и выдает рекомендации, user_..index определяет пользователей для генерации рекомендаций'
    # создаю индекс длиной числа брендов-категорий
    # добавляю туда все вектора по юзерам
    if method == 'faiss':
        if metric == 'euclid':
            user_item_train = user_item_cut.loc[~user_item_cut.index.isin(user_item_cut_index)]
            user_item_test = user_item_cut.loc[user_item_cut.index.isin(user_item_cut_index)]
        elif metric == 'cosine':
            user_item_cut_normalized = user_item_cut.div(user_item_cut.sum(axis=1), axis=0)
            user_item_train = user_item_cut_normalized.loc[~user_item_cut_normalized.index.isin(user_item_cut_index)]
            user_item_test = user_item_cut_normalized.loc[user_item_cut_normalized.index.isin(user_item_cut_index)]
            del user_item_cut_normalized

    if method == 'faiss':

        if metric == 'euclid':
            index = faiss.IndexFlatL2(user_item_cut.shape[1], )

            user_item_array = np.array(user_item_train).astype('float32')
            user_item_array_test = np.array(user_item_test).astype('float32')

            if str(user_item_array.flags)[17:22] == 'False' or str(user_item_array_test.flags)[17:22] == 'False':
                user_item_array = user_item_array.copy(order='C')
                user_item_array_test = user_item_array_test.copy(order='C')
#             index.add(user_item_array)
        elif metric == 'cosine':
            index = faiss.IndexFlatIP(user_item_cut.shape[1], )
            user_item_array = np.array(user_item_train).astype('float32')
            user_item_array_test = np.array(user_item_test).astype('float32')

            if str(user_item_array.flags)[17:22] == 'False' or str(user_item_array_test.flags)[17:22] == 'False':
                user_item_array = user_item_array.copy(order='C')
                user_item_array_test = user_item_array_test.copy(order='C')
#             index.add(user_item_array)
        if inference:
            index.add(user_item_array_test)
        else:
            index.add(user_item_array)

        user_dict = {}
        # создаю юзера, для которого будут искаться соседи
        for searched_user_clid in user_item_cut_index:
            searched_user_index = user_item_test.index.get_loc(searched_user_clid)

            searched_user = user_item_array_test[searched_user_index]
            # меняю формат вектора, чтобы подходил для метода поиска
            searched_user = searched_user.reshape((1, searched_user.shape[0]))
            # нахожу k соседей для выбранного юзера
            dist, ind = index.search(searched_user, k=k)
            # оставляю только соседей
            ind_reshape = ind.reshape((k,))
            dist_reshape = dist.reshape((k,))
            #         ind_reshape = ind_reshape[ind_reshape != searched_user_index]
            # нахожу соседей в юзер-айтем матрице, оставляю только столбы с ненулевыми элементами
            found_neighbours = user_item_cut.iloc[ind_reshape, :]
            #             if metric == 'cosine':
            found_neighbours.loc[searched_user_clid] = user_item_cut.loc[searched_user_clid]

            found_neighbours.loc['preferred_bin'] = (found_neighbours.loc[searched_user_clid] > 0).astype(int)
            found_neighbours.loc['preferred_exact'] = found_neighbours.loc[searched_user_clid]
            found_neighbours.loc['recommended_bin'] = (found_neighbours.mean(axis=0) > 0).astype(int)
            found_neighbours.drop(index=[searched_user_clid], inplace=True)
            found_neighbours.loc['recommended'] = found_neighbours.drop(index=['recommended_bin',
                                                                               'preferred_bin',
                                                                               'preferred_exact']).mean(axis=0)
#             found_neighbours.loc['recommended'] = found_neighbours.drop(index=['recommended_bin',
#                                                                                'preferred_bin',
#                                                                                'preferred_exact',
#                                                                                ]).apply(lambda x:
#                                                                                         np.average(x,
#                                                                                                    weights=dist_reshape[1:]),
#                                                                                         axis = 0)

            # found_neighbours = found_neighbours.T
            # found_neighbours[found_neighbours.loc[:,'recommended_bin'] > 0].T
            user_dict[searched_user_clid] = {}
            user_dict[searched_user_clid]['neighbours'] = found_neighbours.iloc[:k, :]
            user_dict[searched_user_clid]['recommends'] = found_neighbours.loc['recommended']
            user_dict[searched_user_clid]['recommends_binary'] = found_neighbours.loc['recommended_bin']
            user_dict[searched_user_clid]['preferred_binary'] = found_neighbours.loc['preferred_bin']
            user_dict[searched_user_clid]['preferred_exact'] = found_neighbours.loc['preferred_exact']
            user_dict[searched_user_clid]['distance'] = dist_reshape

    if method == 'hardcode':
        if metric == 'euclid':
            if inference:
                user_item_train = user_item_cut
            else:
                user_item_train = user_item_cut.loc[~user_item_cut.index.isin(user_item_cut_index)]
                user_item_test = user_item_cut.loc[user_item_cut.index.isin(user_item_cut_index)]
        elif metric == 'cosine':
            user_item_cut_normalized = user_item_cut.div(user_item_cut.sum(axis=1), axis=0)
            if inference:
                user_item_train = user_item_cut_normalized
            else:
                user_item_train = user_item_cut_normalized.loc[~user_item_cut_normalized.index.isin(user_item_cut_index)]
                user_item_test = user_item_cut_normalized.loc[user_item_cut_normalized.index.isin(user_item_cut_index)]
            del user_item_cut_normalized

        user_dict = {}
        for user_ in user_item_cut_index:
            # user_ = '1586517765142996502'
            user_prefs = user_item_cut.loc[user_]
            non_null_prefs = user_item_train.loc[:, user_prefs.loc[user_prefs != 0].index]
            nn = {}
            for user in non_null_prefs.index:
                if user != user_:
                    if metric == 'euclid':
                        distance = euclidean(user_prefs.loc[user_prefs != 0], non_null_prefs.loc[user])
                    if metric == 'cosine':
                        distance = cosine(user_prefs.loc[user_prefs != 0], non_null_prefs.loc[user])
                    nn[user] = distance

            found_neighbours = user_item_train.loc[
                take(k, {k: v for k, v in sorted(nn.items(), key=lambda item: item[1])})]
            distances = pd.Series(nn.values()).sort_values(ascending=True).head(k).to_list()
            recommends = found_neighbours.mean(axis=0)
            user_dict[user_] = {}
            user_dict[user_]['recommends'] = recommends
            user_dict[user_]['preferred_binary'] = (user_item_cut.loc[user_] > 0).astype(int)
            user_dict[user_]['preferred_exact'] = user_item_cut.loc[user_]
            user_dict[user_]['distance'] = distances
    return user_dict