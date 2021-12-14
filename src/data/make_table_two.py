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

cart_full = cart.merge(vygruz.loc[:,['id','categ']], how='left', left_on='product_id', right_on='id')


# ready table two
# то же самое по группам бренд-группа категорий (сколько товаров, сколько просмотров, когда)
brand_categ_info = vygruz.loc[vygruz.reason =='Приемка'].groupby(['brand',
                                                                  'Группа категорий']).agg({'id':'count','id_s':list})
# .loc[vygruz.id_s.isin(df_heat.product_id.unique())] # для включения только тех групп, что просмотрены
brand_categ_info = brand_categ_info.loc[brand_categ_info.id > 0].sort_values('id', ascending = False)
# суммарные просмотры всех вещей из категории+бренда
brand_categ_info['total_views'] = brand_categ_info['id_s'].apply(lambda x:
                                                                 df_heat.loc[df_heat.product_id.isin(x)]['_id'].count())
# среднее число просмотров одной вещи
brand_categ_info['mean_views'] = round(brand_categ_info['total_views'] / brand_categ_info['id'],2)
# список времен просмотров (можно сделать еще доп. столбец с относительной новизной)
brand_categ_info['view_times'] = brand_categ_info['id_s'].apply(lambda x:
                                                                df_heat.loc[df_heat.product_id.isin(x)]['timestamp'].to_list())



print(brand_categ_info)
brand_categ_info.to_csv('brand_category_info.csv')