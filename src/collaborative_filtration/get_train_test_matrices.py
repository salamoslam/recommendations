import sys
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.append('/src/data')

# from src.data.get_users_info import *
# from src.data.get_brand_category_info import *
from src.data.get_preference_matrix import *
import pandas as pd

def cut_user_item(item_user, threshold = 3):
    "оставляет только людей, которые посмотрели больше threshold категорий, можно улучшить и поставить относительно суммы по чуваку"
    user_item = item_user.T
    user_item_bin = (user_item > 0).astype(int)
    user_item_bin = user_item_bin.loc[user_item_bin.sum(axis = 1)>threshold]

    user_item_cut = user_item.loc[user_item.index.isin(user_item_bin.index)]
    item_user_cut = user_item_cut.T

    return user_item_cut


def get_old_user_item(user_item_cut, days_back=7, threshold=3, grouped = True):
    "достает юзер-айтем на days_back дней назад и обрезает ее до заданного числа активностей, по сути это почти трейн"
    # подгружаем старую юзер-айтем
    item_user_old = get_pref_matrix(to_csv=False, days_back=days_back, grouped=grouped)
    user_item_old = item_user_old
    if grouped:
        user_item_old = user_item_old.drop(columns=['id_s', 'id_list', 'item_total'], index=['user_total']).T

    # обрезаем неактивных чуваков
    user_item_old_cut = cut_user_item(item_user=user_item_old.T, threshold=threshold)
    new_index = user_item_old_cut.index
    # добавляем возможно новые появившиеся категории и заполняем нулями
    user_item_old_cut = user_item_old_cut.reindex(columns=user_item_cut.columns, fill_value=0)
    #     if not grouped:
    user_item_old_cut.index = new_index
    return user_item_old_cut

def get_binarized_differences(user_item_cut, user_item_old_cut, do_first = 'binarize'):
    "трейн выборка для первой метрики: те, у кого разность между бинаризованными векторами предпочтений ненулевая \
    или бинаризованная разность ненулевая"

    assert do_first in ['binarize','subtract']
    user_item_diff = pd.DataFrame(columns=user_item_cut.columns)
    users_with_new_prefs = []
    for user in user_item_old_cut.index:
        #тут сначала бинаризовать а потом вычитать только!!!!!!!
        if do_first == 'binarize':
            new_prefs = (user_item_cut.loc[user]>0).astype(int) - (user_item_old_cut.loc[user]>0).astype(int)
        if do_first == 'subtract':
        # тут, логично, больше будет валидационная выборка
            new_prefs = ((user_item_cut.loc[user] - user_item_old_cut.loc[user])>0).astype(int)
        if new_prefs.sum() > 0:
            users_with_new_prefs.append(user)
            user_item_diff = user_item_diff.append(new_prefs)

    return user_item_diff