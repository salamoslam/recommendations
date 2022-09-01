import sys
import pandas as pd
import numpy as np
import datetime
from pathlib import Path
import os

def preprocess_single_table(table:pd.DataFrame, goods:pd.DataFrame) -> pd.DataFrame:
    table_ = table.explode('product_id').reset_index().merge(goods.loc[:,['id','Группа категорий','brand']],
                                               how = 'left',
                                               left_on = 'product_id',
                                               right_on = 'id').drop(columns = ['product_id'])
    table_.loc[:,'brand_categ'] = list(zip(table_['brand'], table_['Группа категорий']))
    table_.drop(columns=['Группа категорий',"brand"], inplace=True)
    table_ = table_.drop(
        columns=['id']).join(table_.groupby(['ym_client_id','brand_categ'])['id'].transform('count'))
    return table_

path = Path(sys.path[0])
days_back = 0
path_to_repo = str(path.parent.parent.absolute())
import_path =  path_to_repo + '/data/raw/'
export_path = path_to_repo + '/data/interim'

def get_new_pref_matrix(
                    path_to_repo =str(Path(sys.path[0]).parent.parent.absolute()),
                    heat_param = 1,
                    cart_param = 10,
                    wish_param = 5,
                    days_back = 0) -> pd.DataFrame:
    "возвращает юзер-айтем матрицу с заданными параметрами корзин и вишлистов на момент days_back дней назад"

    import_path = path_to_repo + '/data/raw/'
    export_path = path_to_repo + '/data/interim'

    goods = pd.read_excel(import_path+'goods.xlsx')

    # goods = goods.loc[goods.reason == 'Приемка']
    goods = goods.loc[goods.id != '']
    goods = goods.loc[~goods.id.isnull()]
    goods.id = goods.id.astype(str).str.slice(start = 0, stop = -2)
    goods = goods[goods['id'].str.isnumeric()]
    goods['id'] = goods.id.replace('', np.nan, regex=False).astype(str)

    # подрубаем корзины и вишлисты
    cart = pd.read_csv(import_path+'cart.csv', sep = ',',converters={'product_id':str})
    cart.timestamp = pd.to_datetime(cart.timestamp, format='%Y-%m-%d %H:%M:%S')
    cart = cart.loc[cart.timestamp < datetime.datetime.now() - datetime.timedelta(days=days_back)]

    cart['product_id'] = cart['product_id'].replace('', np.nan, regex=False).astype(str)
    cart = cart.dropna(subset=['product_id'])
    cart.loc[:, ['product_id']] = cart.product_id.astype(str)
    cart_user_prod = cart.loc[(~cart.ym_client_id.isin([None, 0, '', '0', 'null']))
                                  & (cart.type == 'C')].groupby('ym_client_id')['product_id'].apply(list).to_frame()
    wish_user_prod = cart.loc[(~cart.ym_client_id.isin([None, 0, '', '0', 'null']))
                                  & (cart.type == 'W')].groupby('ym_client_id')['product_id'].apply(list).to_frame()


    user_carts = preprocess_single_table(cart_user_prod, goods)
    user_carts.set_index(['ym_client_id','brand_categ'], inplace=True)
    user_wishlists = preprocess_single_table(wish_user_prod, goods)
    user_wishlists.set_index(['ym_client_id','brand_categ'], inplace=True)

    # подрубаем хиты
    df_heat = pd.read_csv(import_path+'heats.csv', sep = ',', converters={'product_id':str,'ym_client_id':str})
    df_heat.timestamp = pd.to_datetime(df_heat.timestamp, format='%Y-%m-%d %H:%M:%S')
    df_heat = df_heat.loc[df_heat.timestamp < datetime.datetime.now() - datetime.timedelta(days=days_back)]
    df_heat['product_id'] = df_heat['product_id'].str.rstrip('.0')
    products = df_heat.loc[
            (~(df_heat.product_id.isin(['0', '']))) & (df_heat.product_id != '16777215')]  # выкидываю технические неполадки
    del df_heat
    products['ym_client_id'] = products['ym_client_id'].astype(str)
    # products.product_id = products.product_id.astype(int)
    products = products.loc[products.product_id != 'NaN']
    user_product_heat = products.groupby('ym_client_id')['product_id'].apply(list).to_frame()
    del products
    user_heats = preprocess_single_table(user_product_heat,goods)
    user_heats.set_index(['ym_client_id','brand_categ'], inplace=True)

    # разбитая на три части юзер-айтем матрица
    # heats_third = user_heats.reset_index().pivot_table(index='ym_client_id',columns='brand_categ',values = 'id').fillna(0)
    # carts_third = user_carts.reset_index().pivot_table(index='ym_client_id',columns='brand_categ',values = 'id').fillna(0)
    # wishlists_third = user_wishlists.reset_index().pivot_table(index='ym_client_id',columns='brand_categ',values = 'id').fillna(0)

    triple = user_heats.join(user_carts.join(user_wishlists, lsuffix = '_c', how='outer'), lsuffix = '_h', how = 'outer').reset_index().fillna(0).rename(columns = {'id_h':'heat_count',
                                                                                            'id_c':'cart_counter',
                                                                                            'id':'wishlist_counter'}).drop_duplicates(subset = ['ym_client_id','brand_categ'])

    goods_ = goods.loc[goods.status == 'Не Архив']
    goods_.loc[:,'id_count'] = goods_.groupby(['brand','Группа категорий'])['id'].transform('count')
    goods_ = goods_.loc[goods_.id_count > 1][['brand','Группа категорий']].drop_duplicates()
    brand_categ_stock = list(zip(goods_['brand'], goods_['Группа категорий']))

    triple = triple.loc[triple.brand_categ.isin(brand_categ_stock)]

    triple.loc[:,'sum'] = (heat_param*triple.heat_count + cart_param*triple.cart_counter + wish_param*triple.wishlist_counter).astype('int16')

    user_item = triple.pivot_table(index = 'ym_client_id', columns = 'brand_categ', aggfunc='sum', values = 'sum').fillna(0).astype('int16')
    return user_item, triple

