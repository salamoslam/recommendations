import pandas as pd
import numpy as np
import os
import datetime
from pathlib import Path
import sys

def get_users_info(to_csv = True,
                   path_to_repo=str(Path(sys.path[0]).parent.parent.absolute()),
                   days_back = 0):
    "делает табличку с инфой по пользователям, если to_csv = True, запихивает csv с этой таблицей в data-interim,\
    можно задать путь до локального репозитория"

    import_path = path_to_repo + '/data/raw/'
    export_path = path_to_repo + '/data/interim'

    df_heat = pd.read_csv(import_path+'heats.csv', sep = ',', converters={'product_id':str,'ym_client_id':str})
    cart = pd.read_csv(import_path+'cart.csv', sep = ',',converters={'product_id':str})
    users = pd.read_csv(import_path+'users.csv')
    vygruz = pd.read_excel(import_path+'goods.xlsx').iloc[:,1:]

    # подрубаю время
    df_heat.timestamp = pd.to_datetime(df_heat.timestamp, format='%Y-%m-%d %H:%M:%S')
    df_heat = df_heat.loc[df_heat.timestamp < datetime.datetime.now() - datetime.timedelta(days=days_back)]

    cart.timestamp = pd.to_datetime(cart.timestamp, format='%Y-%m-%d %H:%M:%S')
    cart = cart.loc[cart.timestamp < datetime.datetime.now() - datetime.timedelta(days=days_back)]

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
    vygruz.loc[:, ['id_s']] = vygruz.id.astype(str)

    # теперь аналогично посмотрим на добавления в корзину/вишлисты
    vygruz['id'] = vygruz.id.replace('', np.nan, regex=False).astype(int)
    cart = cart.dropna(subset=['product_id'])
    cart.loc[:, ['product_id']] = cart.product_id.astype(int)

    cart_full = cart.merge(vygruz.loc[:,['id','categ']], how='left', left_on='product_id', right_on='id')


    # ready table one
    # по строчкам юзеры, по столбцам инфа по ним (даты визитов, число просмотров и тд)
    users_info = pd.DataFrame()
    users_info.index = df_heat.ym_client_id.unique()

    # всего переходов по страницам
    users_info['views'] = df_heat.groupby('ym_client_id').agg({'_id': 'count'})
    # список просмотренных товаров
    users_info['products'] = \
    df_heat.loc[~df_heat.product_id.isin([0, '0', ''])].groupby('ym_client_id').agg({'product_id': [list, 'count']})[
        'product_id']['list']
    # кол-во просмотренных товаров (возможны повторные просмотры тех же товаров)
    users_info['products_quan'] = \
    df_heat.loc[~df_heat.product_id.isin([0, '0', ''])].groupby('ym_client_id').agg({'product_id': [list, 'count']})[
        'product_id']['count']
    users_info['products_quan'] = users_info.products_quan.fillna(0)
    # добавления в корзину
    users_info['carts'] = cart_full.loc[(~cart_full.ym_client_id.isin(['null', '', '0'])) & (cart_full.type == 'C')
                                        ].groupby('ym_client_id').agg({'product_id': [list, 'count']})['product_id'][
        'list']
    # кол-во добавлений в корзину
    users_info['carts_quan'] = cart_full.loc[(~cart_full.ym_client_id.isin(['null', '', '0'])) & (cart_full.type == 'C')
                                             ].groupby('ym_client_id').agg({'product_id': [list, 'count']})[
        'product_id']['count']
    users_info['carts_quan'] = users_info.carts_quan.fillna(0)

    # добавления в вишлист
    users_info['wish'] = cart_full.loc[(~cart_full.ym_client_id.isin(['null', '', '0'])) & (cart_full.type == 'W')
                                       ].groupby('ym_client_id').agg({'product_id': [list, 'count']})['product_id'][
        'list']
    # кол-во добавлений в вишлист
    users_info['wish_quan'] = cart_full.loc[(~cart_full.ym_client_id.isin(['null', '', '0'])) & (cart_full.type == 'W')
                                            ].groupby('ym_client_id').agg({'product_id': [list, 'count']})[
        'product_id']['count']
    users_info['wish_quan'] = users_info.wish_quan.fillna(0)
    if to_csv:
        users_info.to_csv(os.path.join(export_path,r'users_info.csv'))
    return users_info

export_path = str(Path(sys.path[0]).parent.parent) + '/data/interim'

users_info_old = get_users_info(days_back=30, to_csv=False)
users_info_old.to_csv(os.path.join(export_path,'users_info_old.csv'))

users_info = get_users_info(days_back=0, to_csv=False)
users_info.to_csv(os.path.join(export_path,'users_info.csv'))