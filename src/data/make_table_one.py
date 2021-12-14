import pandas as pd
import numpy as np

df_heat = pd.read_csv('heats.csv')
cart = pd.read_csv('cart.csv')
users = pd.read_csv('users.csv')
vygruz = pd.read_excel('downloaded_file1.xlsx').iloc[:,1:]

vygruz = vygruz.loc[~vygruz.id.isnull()]
vygruz = vygruz[vygruz['id'].str.isnumeric()]
# print(vygruz.info())
vygruz['id'] = vygruz.id.replace('',np.nan, regex=False).astype(int)
cart = cart.dropna(subset = ['product_id'])
cart.loc[:,['product_id']] = cart.product_id.astype(int)

cart_full = cart.merge(vygruz.loc[:,['id','categ']], how='left', left_on='product_id', right_on='id')


# ready table one
# по строчкам юзеры, по столбцам инфа по ним (даты визитов, число просмотров и тд)
users_info = pd.DataFrame()
users_info.index = df_heat.ym_client_id.unique()

# всего переходов по страницам
users_info['views'] = df_heat.groupby('ym_client_id').agg({'_id':'count'})
#список просмотренных товаров
users_info['products'] = df_heat.loc[~df_heat.product_id.isin([0,'0'])].groupby('ym_client_id').agg({'product_id':[list,'count']})['product_id']['list']
#кол-во просмотренных товаров (возможны повторные просмотры тех же товаров)
users_info['products_quan'] = df_heat.loc[~df_heat.product_id.isin([0,'0'])].groupby('ym_client_id').agg({'product_id':[list,'count']})['product_id']['count']
#добавления в корзину
users_info['carts'] = cart_full.loc[(~cart_full.ym_client_id.isin(['null','','0']))&(cart_full.type=='C')
             ].groupby('ym_client_id').agg({'product_id':[list,'count']})['product_id']['list']
#кол-во добавлений в корзину
users_info['carts_quan'] = cart_full.loc[(~cart_full.ym_client_id.isin(['null','','0']))&(cart_full.type=='C')
             ].groupby('ym_client_id').agg({'product_id':[list,'count']})['product_id']['count']

#добавления в вишлист
users_info['wish'] = cart_full.loc[(~cart_full.ym_client_id.isin(['null','','0']))&(cart_full.type=='W')
             ].groupby('ym_client_id').agg({'product_id':[list,'count']})['product_id']['list']
#кол-во добавлений в вишлист
users_info['wish_quan'] = cart_full.loc[(~cart_full.ym_client_id.isin(['null','','0']))&(cart_full.type=='W')
             ].groupby('ym_client_id').agg({'product_id':[list,'count']})['product_id']['count']

users_info.to_csv('users_info.csv')