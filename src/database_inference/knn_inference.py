import sys
from pathlib import Path
rec_path = str(Path(sys.path[0]).parent.parent.absolute())
sys.path.append(rec_path +'/src/data')

sys.path.append(rec_path +'/src/data')
sys.path.append(rec_path +'/src/collaborative_filtration')
sys.path.append(rec_path +'/src/database_inference')
from get_tables import *
from get_preference_matrix import *
from get_train_test_matrices import *
from nearest_neighbours import *
from collaborative_inference import *
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