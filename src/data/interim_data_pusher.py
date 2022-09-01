## файл объединяет функции из get_users_info, get_brand_category_info, get_new_preference_matrix
## и засовывает предобработанные таблички для градиентного бустинга в базу и папку data -> interim

from get_new_preference_matrix import *
from get_users_info import *
from get_brand_category_info import *
from pymongo import MongoClient
import certifi
import warnings
warnings.filterwarnings('ignore')

def dict_to_collection(dictt:dict, user_item = False) -> list:
    "Преобразует словарь из датафрейма в монговскую коллекцию,\
    формат словаря: {'_id1': {'feature1': value1, 'feature2': value2}, '_id2': {...}, ...}"

    col = []

    for _id, features in dictt.items():
        lil_dict = {}
        lil_dict['_id'] = str(_id)
        for feature, value in features.items():
            lil_dict[feature] = value
        col.append(lil_dict)
    if user_item:
        col.append({k:0 for k, v in lil_dict.items()})
    return col

def push_col_to_mongo(col_name:str, col:list, current_db):
    'Принимает коллекцию и запихивает ее в базу с названием col_name'

    pushable_cols = current_db.list_collection_names()
    assert col_name in pushable_cols

    col_db = current_db[col_name]
    col_db.delete_many({})
    col_db.insert_many(col)

    return len(col_db.distinct('_id'))

path_to_repo = str(Path(sys.path[0]).parent.parent)
with open(path_to_repo+'/src/data/mongodb_pass.txt', 'r') as file:
    path2 = file.read()

client = MongoClient(path2,  tlsCAFile=certifi.where())
current_db = client['spin_services']
export_path = path_to_repo + '/data/interim/'

# подгружаем текущую юзер-айтем
user_item, triple = get_new_pref_matrix()

# пушим в монгу user_item
user_item.columns = [str(i) for i in user_item.columns]
user_item_dict = user_item.T.to_dict()
user_item_col = dict_to_collection(user_item_dict)
push_col_to_mongo('user_item', user_item_col, current_db=current_db)

# user_item.to_csv(os.path.join(export_path,'user_group_info.csv'))
# triple.to_csv(os.path.join(export_path,'user_group_melt.csv'))



# подгружаем старую юзер-айтем
user_item, triple = get_new_pref_matrix(days_back=30)

# user_item.to_csv(os.path.join(export_path,'user_group_info_old.csv'))
# triple.to_csv(os.path.join(export_path,'user_group_melt_old.csv'))

# запихнуть свежую матрицу взаимодействий в базу




#########
##### подгружаем текущую инфу по юзерам
users_info = get_users_info(days_back=0, to_csv=False, path_to_repo=str(Path(sys.path[0]).parent.parent))
users_info.to_csv(os.path.join(export_path,'users_info.csv'))

users_info_dict = users_info.T.to_dict()
users_info_col = dict_to_collection(users_info_dict)
push_col_to_mongo('users_info', users_info_col, current_db=current_db)

## подгружаем старую инфу по юзерам
# users_info_old = get_users_info(days_back=30, to_csv=False, path_to_repo=str(Path(sys.path[0]).parent.parent))
# users_info_old.to_csv(os.path.join(export_path,'users_info_old.csv'))
### запихнуть свежую инфу по юзерам в базу



############
# подгружаем текущую инфу по брендам-категориям
brand_category_info = get_brand_category_info(days_back=0, to_csv=False)
# brand_category_info.to_csv(os.path.join(export_path,'brand_category_info.csv'))


brand_categ_dict = brand_category_info.set_index('brand_categ').T.to_dict()
brand_categ_col = dict_to_collection(brand_categ_dict)
push_col_to_mongo('brand_category_info', brand_categ_col, current_db=current_db)

# подгружаем старую инфу под брендам-категориям
# brand_category_info_old = get_brand_category_info(days_back=30, to_csv=False)
# brand_category_info_old.to_csv(os.path.join(export_path,'brand_category_info_old.csv'))

#### запихиваем свежие фичи брендов-категорий в базу
