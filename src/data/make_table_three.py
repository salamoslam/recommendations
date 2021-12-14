import pandas as pd
import numpy as np

df_heat = pd.read_csv('heats.csv')
cart = pd.read_csv('cart.csv')
users = pd.read_csv('users.csv')
vygruz = pd.read_excel('downloaded_file1.xlsx').iloc[:,1:]

vygruz = vygruz.loc[~vygruz.id.isnull()]
vygruz = vygruz[vygruz['id'].str.isnumeric()]
vygruz.loc[:,['id_s']] = vygruz.id.astype(str)
# print(vygruz.info())
vygruz['id'] = vygruz.id.replace('',np.nan, regex=False).astype(int)
cart = cart.dropna(subset = ['product_id'])
cart.loc[:,['product_id']] = cart.product_id.astype(int)

products = df_heat.loc[(df_heat.product_id != '0')&(df_heat.product_id != '16777215')] #выкидываю технические неполадки


# задаются параметры для заполнения матрицы предпочтений
heat_param = 1
cart_param = 10
wish_param = 5



# достаю списки товаров из корзин/вишлистов/заказов каждого клиента
cart_user_prod = cart.loc[(~cart.ym_client_id.isin([None,0,'','0','null']))
                          &(cart.type == 'W')].groupby('ym_client_id')['product_id'].apply(list).to_frame()
wish_user_prod = cart.loc[(~cart.ym_client_id.isin([None,0,'','0','null']))
                          &(cart.type == 'C')].groupby('ym_client_id')['product_id'].apply(list).to_frame()
# засовываю в единый фрейм с учетом непересечений
cart_wish_user_prod = cart_user_prod.join(wish_user_prod,
                                          how = 'outer',
                                          on ='ym_client_id',
                                          lsuffix = '_cart',
                                          rsuffix = '_wish').drop_duplicates('ym_client_id').reset_index().drop(columns = 'index')





# создаю матрицу предпочтений по трем видам (пока что) оценок: просмотр товара, добавление в корзину/вишлист, добавление в заказ
products['ym_client_id'] = products['ym_client_id'].astype(str)
products = products.loc[products.product_id != 'NaN']

# делаю список со всеми возможными вещами, обрезать по наличию буду потом
all_items = np.concatenate([products.product_id.unique(), cart.product_id.astype(str).unique()])
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

# после этого, по сути, надо дропнуть вещи, которые уже архивные

combinations = {
    1:['categ'],
    2:['brand'],
    3:['categ','brand'],
    4:['brand','Группа категорий']
}
dict_combs = {}
for num, comb in combinations.items():
    cols = ['id_s']
    cols.extend(comb)
    brand_categ = user_product_heat.T.merge(vygruz.loc[:, cols], how='left', left_on=user_product_heat.T.index,
                                            right_on='id_s')
    stock = vygruz.loc[vygruz.reason == 'Приемка'].groupby(comb)['id_s'].count()

    dict_combs[num] = brand_categ.groupby(comb).sum()
    dict_combs[num] = dict_combs[num].join(stock)
    dict_combs[num] = dict_combs[num].loc[~((dict_combs[num].id_s.isna()) | (dict_combs[num].id_s == 1))]

print(dict_combs[4])
dict_combs[4].to_csv('user_group_info.csv')