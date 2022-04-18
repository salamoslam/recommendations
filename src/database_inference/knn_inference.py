import sys
sys.path.insert(1, '../src/data')
sys.path.append('../src/collaborative_filtration')
sys.path.append('../src/database_inference')
from src.data.get_tables import *
from src.data.get_preference_matrix import *
from src.collaborative_filtration.get_train_test_matrices import *
from src.collaborative_filtration.nearest_neighbours import *
from src.database_inference.collaborative_inference import *
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

path = Path(sys.path[0])
path_to_repo = str(path.parent.parent.parent.absolute())
# path_to_repo = '/Users/kuznetsovnikita'

pref_matrix = get_pref_matrix(path_to_repo= path_to_repo, to_csv=False)
item_user = pref_matrix.drop(columns=['id_s','id_list','item_total'], index = ['user_total'])

user_item_cut = cut_user_item(item_user)

user_dict_ = recommend_NN(user_item_cut=user_item_cut,
                          user_item_cut_index=user_item_cut.index,
                          k=20,
                          method='faiss',
                          metric = 'euclid',
                          inference=True)

id_dict = pref_matrix.id_list.drop(index='user_total').to_dict()

# подрубаемся к базе и запихиваем туда рекомендации

with open(path_to_repo+'/mongodb_pass.txt', 'r') as file:
    path2 = file.read()

client = MongoClient(path2,  tlsCAFile=certifi.where())
current_db = client['spin_services']

recommends_collection = get_recommends_collection(user_dict_, current_db, id_dict)

recommends_col_db = current_db['recommends']
recommends_col_db.delete_many({})
recommends_col_db.insert_many(recommends_collection)