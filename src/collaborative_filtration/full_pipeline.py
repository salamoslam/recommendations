import sys
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.insert(1, '../src/data')
sys.path.append('../src/collaborative_filtration')
sys.path.append('../src/database_inference')

from src.data.get_users_info import *
from src.data.get_brand_category_info import *
from src.data.get_preference_matrix import *
from src.data.get_popular_items import *
import implicit
import faiss
from tqdm import tqdm
from scipy.sparse import csr_matrix
from scipy.spatial.distance import euclidean, cosine
from sklearn.metrics import precision_score
from itertools import islice
from pymongo import MongoClient
import certifi
from get_train_test_matrices import *
from validation_pipeline import *
from validation_pipeline import *
from src.database_inference.collaborative_inference import *



import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_columns', 1000)
# pd.set_option('display.max_rows', 1000)

# матрица предпочтений
pref_matrix = get_pref_matrix(to_csv=False)
item_user = pref_matrix.drop(columns=['id_s','id_list','item_total'], index = ['user_total'])

users_info = get_users_info(to_csv=False)

user_item_cut = cut_user_item(item_user)

user_item_old_cut = get_old_user_item(user_item_cut, days_back=10)

user_item_diff = get_binarized_differences(user_item_cut, user_item_old_cut, do_first='binarize')

# user_item_diff.shape

# get_validation_plots(user_item_cut, user_item_old_cut, user_item_diff)

user_dict_ = recommend_NN(user_item_cut=user_item_cut,
                          user_item_cut_index=user_item_cut.index,
                          k=20,
                          method='faiss',
                          metric = 'euclid',
                          inference=True)

id_dict = pref_matrix.id_list.drop(index='user_total').to_dict()

path_to_repo = '/Users/kuznetsovnikita/recommendations'
with open(path_to_repo+'/src/data/mongodb_pass.txt', 'r') as file:
    path2 = file.read()

client = MongoClient(path2,  tlsCAFile=certifi.where())
current_db = client['spin_services']

recommends_collection = get_recommends_collection(user_dict_, current_db, id_dict)

recommends_col_db = current_db['recommends']
recommends_col_db.delete_many({})
recommends_col_db.insert_many(recommends_collection)