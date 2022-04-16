import pandas as pd
import numpy as np
import os
import datetime

path_to_repo = '/Users/kuznetsovnikita'


def get_pref_matrix(to_csv = True,
                    path_to_repo='/Users/kuznetsovnikita',
                    heat_param = 1,
                    cart_param = 10,
                    wish_param = 5,
                    days_back = 0,
                    grouped = True):
    "делает матрицу предпочтений, если to_csv = True, запихивает csv с этой таблицей в data-interim\
    можно задать параметры предпочтений и путь до локального репозитория"

    import_path = path_to_repo + '/recommendations/data/raw/'
    export_path = path_to_repo + '/recommendations/data/interim'

    df_heat = pd.read_csv(import_path+'heats.csv', sep = ',', converters={'product_id':str,'ym_client_id':str})
    cart = pd.read_csv(import_path+'cart.csv', sep = ',',converters={'product_id':str})
    users = pd.read_csv(import_path+'users.csv')
    vygruz = pd.read_excel(import_path+'goods.xlsx').iloc[:,1:]

    # задаю время взятия данных
    cart.timestamp = pd.to_datetime(cart.timestamp, format='%Y-%m-%d %H:%M:%S')
    cart = cart.loc[cart.timestamp < datetime.datetime.now() - datetime.timedelta(days=days_back)]

    df_heat.timestamp = pd.to_datetime(df_heat.timestamp, format='%Y-%m-%d %H:%M:%S')
    df_heat = df_heat.loc[df_heat.timestamp < datetime.datetime.now() - datetime.timedelta(days=days_back)]

    cart['product_id'] = cart['product_id'].replace('', np.nan, regex=False).astype(float)
    vygruz.cumdate = pd.to_datetime(vygruz.cumdate, infer_datetime_format=True)
    vygruz.loc[:, ['days_in_stock']] = (datetime.datetime.now() - vygruz.cumdate).dt.days
    vygruz.loc[:, ['discount']] = np.where(vygruz['Цена со скидкой'].isna(), 0,
                                           (vygruz['Цена шоурум'] - vygruz['Цена со скидкой']) / vygruz['Цена шоурум'])
    vygruz = vygruz.loc[vygruz.id != '']
    # vygruz.id = vygruz.id.replace(' ','',regex=True)

    vygruz = vygruz.loc[~vygruz.id.isnull()]
    vygruz = vygruz[vygruz['id'].str.isnumeric()]
    vygruz['id'] = vygruz.id.replace('', np.nan, regex=False).astype(int)

    ## потоварная статистика
    df_heat['product_id'] = df_heat['product_id'].str.rstrip('.0')
    products = df_heat.loc[
        (~(df_heat.product_id.isin(['0', '']))) & (df_heat.product_id != '16777215')]  # выкидываю технические неполадки


    vygruz.loc[:, ['id_s']] = vygruz.id.astype(str)

    # теперь аналогично посмотрим на добавления в корзину/вишлисты
    vygruz['id'] = vygruz.id.replace('', np.nan, regex=False).astype(int)
    cart = cart.dropna(subset=['product_id'])
    cart.loc[:, ['product_id']] = cart.product_id.astype(int)



    # достаю списки товаров из корзин/вишлистов/заказов каждого клиента
    cart_user_prod = cart.loc[(~cart.ym_client_id.isin([None, 0, '', '0', 'null']))
                              & (cart.type == 'W')].groupby('ym_client_id')['product_id'].apply(list).to_frame()
    wish_user_prod = cart.loc[(~cart.ym_client_id.isin([None, 0, '', '0', 'null']))
                              & (cart.type == 'C')].groupby('ym_client_id')['product_id'].apply(list).to_frame()
    # засовываю в единый фрейм с учетом непересечений
    cart_wish_user_prod = cart_user_prod.join(wish_user_prod,
                                              how='outer',
                                              on='ym_client_id',
                                              lsuffix='_cart',
                                              rsuffix='_wish').drop_duplicates('ym_client_id').reset_index().drop(
        columns='index')



    # создаю матрицу предпочтений по трем видам (пока что) оценок: просмотр товара, добавление в корзину/вишлист, добавление в заказ
    products['ym_client_id'] = products['ym_client_id'].astype(str)
    products = products.loc[products.product_id != 'NaN']

    # делаю список со всеми возможными вещами, обрезать по наличию буду потом
    all_items = np.unique(np.concatenate([products.product_id, cart.product_id.astype(str)]))
    product_id_cols = [str(i) for i in all_items]
    product_id_cols.extend(['product_id_wish', 'product_id_cart', 'product_id', 'ym_client_id'])
    product_id_cols.reverse()

    user_product_heat = products.groupby('ym_client_id')['product_id'].apply(list).reset_index()
    user_product_heat = user_product_heat.merge(cart_wish_user_prod, how='outer', on='ym_client_id')
    user_product_heat = user_product_heat.reindex(columns=product_id_cols, fill_value=0).set_index('ym_client_id')
    user_product_heat = user_product_heat.loc[:, ~user_product_heat.columns.duplicated()]
    user_product_heat.fillna({'product_id': '', 'product_id_cart': '', 'product_id_wish': ''}, inplace=True)

    # заполняю матрицу предпочтений ранжированными значениями (!!запихнуть в функцию потом!!)
    for i in range(user_product_heat.shape[0]):
        if type(user_product_heat.loc[:, 'product_id'].iat[i]) == list:
            for product in user_product_heat.loc[:, 'product_id'].iat[i]:
                user_product_heat[f'{product}'].iat[i] += heat_param
        if type(user_product_heat.loc[:, 'product_id_cart'].iat[i]) == list:
            for product in user_product_heat.loc[:, 'product_id_cart'].iat[i]:
                user_product_heat[f'{product}'].iat[i] += cart_param
        if type(user_product_heat.loc[:, 'product_id_wish'].iat[i]) == list:
            for product in user_product_heat.loc[:, 'product_id_wish'].iat[i]:
                user_product_heat[f'{product}'].iat[i] += wish_param

    user_product_heat.drop(columns=['product_id', 'product_id_cart', 'product_id_wish'], inplace=True)

    combinations = {
        #     1:['categ'],
        #     2:['brand'],
        #     3:['categ','brand'],
        4: ['brand', 'Группа категорий']
    }
    dict_combs = {}
    for num, comb in combinations.items():
        cols = ['id_s']
        cols.extend(comb)
        brand_categ = user_product_heat.T.merge(vygruz.loc[:, cols], how='left', left_on=user_product_heat.T.index,
                                                right_on='id_s')
        stock = vygruz.loc[vygruz.reason == 'Приемка'].groupby(comb).agg({'id_s': ['count', list]})

        dict_combs[num] = brand_categ.groupby(comb).sum()
        dict_combs[num] = dict_combs[num].join(stock)

        dict_combs[num].rename(columns={('id_s', 'count'): 'id_s', ('id_s', 'list'): 'id_list'}, inplace=True)

        dict_combs[num] = dict_combs[num].loc[~((dict_combs[num].id_s.isna()) | (dict_combs[num].id_s == 1))]

        dict_combs[num].loc['user_total'] = dict_combs[num].drop(columns=['id_s', 'id_list']).sum(axis=0)
        dict_combs[num].loc[:, ['item_total']] = dict_combs[num].drop(columns=['id_s', 'id_list']).sum(axis=1)
    #     dict_combs[tuple(comb)].append(dict_combs[tuple(comb)].sum(numeric_only=True), ignore_index=True)

    # print(dict_combs[4])
    if to_csv:
        dict_combs[4].to_csv(os.path.join(export_path,'user_group_info.csv'))
    if grouped:
        return dict_combs[4]
    else:
        return user_product_heat




# чекер на заполненность
# dict_combs[4].drop(columns=['id_s', 'item_total'], index=['user_total']).values.mean()


