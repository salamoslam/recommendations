import pandas as pd
import numpy as np
import os
import datetime
from pathlib import Path
import sys

path_to_repo = '/Users/kuznetsovnikita'
def get_brand_category_info(to_csv = True,
                            path_to_repo = str(Path(sys.path[0]).parent.parent.absolute()),
                            days_back = 0):
    "делает табличку с инфой по бренду-группе категорий, если to_csv = True, запихивает csv с этой таблицей в data-interim,\
    можно задать путь до локального репозитория"

    import_path = path_to_repo + '/data/raw/'
    export_path = path_to_repo + '/data/interim'

    df_heat = pd.read_csv(import_path + 'heats.csv')
    cart = pd.read_csv(import_path + 'cart.csv')
    users = pd.read_csv(import_path + 'users.csv')
    vygruz = pd.read_excel(import_path + 'goods.xlsx').iloc[:, 1:]

    # подбираем время
    df_heat.timestamp = pd.to_datetime(df_heat.timestamp, format='%Y-%m-%d %H:%M:%S')
    df_heat = df_heat.loc[df_heat.timestamp < datetime.datetime.now() - datetime.timedelta(days=days_back)]

    cart.timestamp = pd.to_datetime(cart.timestamp, format='%Y-%m-%d %H:%M:%S')
    cart = cart.loc[cart.timestamp < datetime.datetime.now() - datetime.timedelta(days=days_back)]

    vygruz = vygruz.loc[~vygruz.id.isnull()]
    vygruz.id = vygruz.id.astype(str).str.slice(start = 0, stop = -2)
    vygruz = vygruz[vygruz['id'].str.isnumeric()]
    vygruz.loc[:, ['id_s']] = vygruz.id.astype(str)
    # print(vygruz.info())
    vygruz['id'] = vygruz.id.replace('', np.nan, regex=False).astype(int)
    cart = cart.dropna(subset=['product_id'])
    cart.loc[:, ['product_id']] = cart.product_id.astype(int)

    cart_full = cart.merge(vygruz.loc[:, ['id', 'categ']], how='left', left_on='product_id', right_on='id')

    df_heat['product_id'] = df_heat['product_id'].astype(str).str.rstrip('.0')

    # ready table two
    # то же самое по группам бренд-группа категорий (сколько товаров, сколько просмотров, когда)
    brand_categ_info = vygruz.loc[vygruz.reason == 'Приемка'].groupby(['brand',
                                                                       'Группа категорий']).agg({'id': 'count',
                                                                                                 'id_s': list,
                                                                                                 'Цена шоурум': [
                                                                                                     np.mean,
                                                                                                     min,
                                                                                                     max]})
    brand_categ_info.columns = [' '.join(col).strip() for col in brand_categ_info.columns.values]
    # .loc[vygruz.id_s.isin(df_heat.product_id.unique())] # для включения только тех групп, что просмотрены
    brand_categ_info = brand_categ_info.loc[brand_categ_info['id count'] > 0].sort_values('id count', ascending=False)

    # суммарные просмотры всех вещей из категории+бренда
    brand_categ_info['total_views'] = brand_categ_info['id_s list'].apply(lambda x:
                                                                          df_heat.loc[df_heat.product_id.isin(x)][
                                                                              '_id'].count())
    # среднее число просмотров одной вещи
    brand_categ_info['mean_views'] = round(brand_categ_info['total_views'] / brand_categ_info['id count'], 2)
    # список времен просмотров (можно сделать еще доп. столбец с относительной новизной)
    brand_categ_info['view_times'] = brand_categ_info['id_s list'].apply(lambda x:
                                                                         df_heat.loc[df_heat.product_id.isin(x)]['timestamp'].to_list())
    # print(brand_categ_info)
    if to_csv:
        brand_categ_info.to_csv(os.path.join(export_path,'brand_category_info.csv'))
    return brand_categ_info

export_path = str(Path(sys.path[0]).parent.parent) + '/data/interim'

brand_category_info = get_brand_category_info(days_back=0, to_csv=False)
brand_category_info.to_csv(os.path.join(export_path,'brand_category_info.csv'))

brand_category_info_old = get_brand_category_info(days_back=30, to_csv=False)
brand_category_info_old.to_csv(os.path.join(export_path,'brand_category_info_old.csv'))