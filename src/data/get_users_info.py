import pandas as pd
import numpy as np
import os
import datetime

path_to_repo = '/Users/kuznetsovnikita'
def get_users_info(to_csv = True, path_to_repo='/Users/kuznetsovnikita'):
    "делает табличку с инфой по пользователям, если to_csv = True, запихивает csv с этой таблицей в data-interim,\
    можно задать путь до локального репозитория"

    import_path = path_to_repo + '/recommendations/data/raw/'
    export_path = path_to_repo + '/recommendations/data/interim'

    df_heat = pd.read_csv(import_path+'heats.csv', sep = ',', converters={'product_id':str,'ym_client_id':str})
    cart = pd.read_csv(import_path+'cart.csv', sep = ',',converters={'product_id':str})
    users = pd.read_csv(import_path+'users.csv')
    vygruz = pd.read_excel(import_path+'goods.xlsx').iloc[:,1:]

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
    # добавления в корзину
    users_info['carts'] = cart_full.loc[(~cart_full.ym_client_id.isin(['null', '', '0'])) & (cart_full.type == 'C')
                                        ].groupby('ym_client_id').agg({'product_id': [list, 'count']})['product_id'][
        'list']
    # кол-во добавлений в корзину
    users_info['carts_quan'] = cart_full.loc[(~cart_full.ym_client_id.isin(['null', '', '0'])) & (cart_full.type == 'C')
                                             ].groupby('ym_client_id').agg({'product_id': [list, 'count']})[
        'product_id']['count']

    # добавления в вишлист
    users_info['wish'] = cart_full.loc[(~cart_full.ym_client_id.isin(['null', '', '0'])) & (cart_full.type == 'W')
                                       ].groupby('ym_client_id').agg({'product_id': [list, 'count']})['product_id'][
        'list']
    # кол-во добавлений в вишлист
    users_info['wish_quan'] = cart_full.loc[(~cart_full.ym_client_id.isin(['null', '', '0'])) & (cart_full.type == 'W')
                                            ].groupby('ym_client_id').agg({'product_id': [list, 'count']})[
        'product_id']['count']

    if to_csv:
        users_info.to_csv(os.path.join(export_path,r'users_info.csv'))
    return users_info, cart_full