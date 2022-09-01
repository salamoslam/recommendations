import sys
import lightgbm
import pandas as pd
import numpy as np
from pymongo import MongoClient
import certifi

sys.path.append('../src/data')

def get_test_dset(clid: str, id:str, current_db) -> pd.DataFrame:
    'Вынимает из базы инфу по юзеру и брендам-категориям и склеивает ее в тестовый датасет'

    # достаем инфу по юзерам
    users_info_cursor = current_db['users_info']

    test_user_info = pd.Series(users_info_cursor.find_one({'_id':clid})).to_frame().T
    test_user_info.rename(columns={'_id':'ym_client_id'}, inplace=True)
    test_user_info.drop(columns={'products','carts','wish'}, inplace=True)

    # переделать айдишники в бренде категории на штрихкоды
    # достаем инфу по брендам-категориям
    brand_categ_cursor = current_db['brand_category_info']

    # test_brand_categ = pd.Series(brand_categ_cursor.find_one({'id_s list': {'$regex':test_id}})).to_frame().T
    test_brand_categ = pd.DataFrame(brand_categ_cursor.find())
    test_brand_categ.rename(columns={'_id':'brand_categ'}, inplace=True)
    test_brand_categ.drop(columns=['id_s list','view_times'], inplace=True)


    #достаем инфу по взаимодействиям
    user_item_cursor = current_db['user_item']

    if clid in user_item_cursor.distinct('_id'):
        test_user_item = pd.Series(user_item_cursor.find_one({'_id':clid})).to_frame()
    else:
        test_user_item = pd.Series(user_item_cursor.find_one({'_id':0})).to_frame()
    test_user_item.rename(columns={'_id':'ym_client_id'}, inplace=True)
    # print(test_user_item.head(10))
    clid_ = test_user_item.loc['_id'].iat[0]
    test_user_item.loc[:,'ym_client_id'] = clid_
    test_user_item.drop(index = '_id', inplace=True)
    test_user_item.reset_index(inplace=True)
    test_user_item.rename(columns={'index':'brand_categ',0:'heat_count'}, inplace=True)

    # собираем полный датасет для одного пользователя
    test_user_full = test_user_item.merge(test_user_info, how = 'left', on = 'ym_client_id').merge(test_brand_categ, how='left', on='brand_categ')
    test_user_full.drop(columns=['id_female','id_full', 'id_male'],inplace=True)

    test_user_full.ym_client_id = test_user_full.ym_client_id.astype('category')
    test_user_full.brand_categ = test_user_full.brand_categ.astype('category')

    cols_old = test_user_full.select_dtypes(include=[np.float64, object]).columns
    # test_user_full = test_user_full.fillna(0)
    test_user_full[cols_old] = test_user_full[cols_old].fillna(0).astype(np.int16)



    # cols_old = test_user_full.select_dtypes(include=[object]).columns
    # test_user_full[cols_old] = test_user_full[cols_old].astype(np.int16)
    test_user_full = test_user_full.loc[:,['ym_client_id', 'brand_categ', 'views', 'products_quan', 'carts_quan',
                                           'wish_quan', 'id count', 'Цена шоурум mean', 'Цена шоурум min',
                                           'Цена шоурум max', 'total_views', 'mean_views', 'heat_count']]
    return test_user_full




def lgb_recommend(clid: str, id:str, current_db, path_to_repo:str) -> list:
    'Выдает список рекомендованных штрихкодов по клиду и штрихкоду'

    model = lightgbm.Booster(model_file=path_to_repo + '/models/'+'lgb_model.txt')

    test_user_full = get_test_dset(clid=clid, id=id, current_db=current_db)
    predictions = pd.Series(model.predict(test_user_full), index=test_user_full.brand_categ)

    # тут можем регулировать число категорий рекомендуемых
    predictions = list(predictions.sort_values(ascending = False)[:20].index.astype(str))

    return predictions





def get_recommended_ids(clid:str, path_to_repo:str, id:str, current_db, gender = 'full') -> str:
    'Достает из базы штрихкоды рекомендованных предотсортированных брендов-категорий для заданного пола \
    и сортирует штрихкоды с повторением после проходки по всем категориям'

    assert gender in ['full', 'male','female']

    recommend_categs = lgb_recommend(clid=clid, id=id, path_to_repo=path_to_repo, current_db=current_db)
    brand_categ_cursor = current_db['brand_category_info']
    recommend_ids = pd.DataFrame(brand_categ_cursor.find({'_id':{'$in':recommend_categs}},{f'id_{gender}':1}))
    recs_id = recommend_ids.set_index('_id').loc[recommend_categs][f'id_{gender}']

    recs_id_sorted = []
    max_len = max([len(l) for l in recs_id])
    for i in range(max_len):
        for list_ in recs_id:
            if i <= len(list_) - 1:
                recs_id_sorted.append(list_[i])

    recs_id_sorted_str = ', '.join(recs_id_sorted)
    return recs_id_sorted_str