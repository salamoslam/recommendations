import pandas as pd
import numpy as np
import os

path_to_repo = '/Users/kuznetsovnikita'
def get_brand_category_info(to_csv = True, path_to_repo='/Users/kuznetsovnikita'):
    "делает табличку с инфой по бренду-группе категорий, если to_csv = True, запихивает csv с этой таблицей в data-interim,\
    можно задать путь до локального репозитория"

    path_to_repo = '/Users/kuznetsovnikita'
    import_path = path_to_repo + '/recommendations/data/raw/'
    export_path = path_to_repo + '/recommendations/data/interim'

    df_heat = pd.read_csv(import_path + 'heats.csv')
    cart = pd.read_csv(import_path + 'cart.csv')
    users = pd.read_csv(import_path + 'users.csv')
    vygruz = pd.read_excel(import_path + 'goods.xlsx').iloc[:, 1:]

    vygruz = vygruz.loc[~vygruz.id.isnull()]
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
                                                                                                     np.mean, min,
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