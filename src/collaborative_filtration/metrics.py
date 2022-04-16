import sys
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.insert(1, '../src/data')
import pandas as pd
from sklearn.metrics import precision_score
import numpy as np


def precision_at_k(user_item_diff: pd.DataFrame,
                   user_item_cut: pd.DataFrame,
                   user_dict: dict,
                   user: str,
                   k=15) -> float:
    "считает для каждого чела из валидационной части выборки метрику precision@k"
    target_bin = user_item_diff.loc[user]
    predict_exact = user_dict[user]['recommends']

    compared = pd.DataFrame([target_bin,
                             user_item_cut.loc[user],
                             user_dict[user]['preferred_binary'],
                             predict_exact], index=['fut', 'pref_exact', 'prev', 'predict_exact']).T
    compared.loc[:, 'target_exact_new'] = np.where(compared.prev == 0, compared.pref_exact, 0)
    compared.loc[:, 'predict_exact_new'] = np.where(compared.prev == 0, compared.predict_exact, 0)

    y_true = compared.sort_values('target_exact_new', ascending=False).iloc[:k, :]['fut']
    y_pred = np.where(compared.sort_values('target_exact_new', ascending=False).iloc[:k, :]['predict_exact_new'] > 0, 1,
                      0)
    acc = precision_score(y_true, y_pred)
    return acc


def recall_at_k(user_item_diff: pd.DataFrame,
                user_dict: dict,
                user: str,
                k=15) -> float:
    "считает для каждого чела из валидационной части выборки метрику recall@k"
    target_bin = user_item_diff.loc[user]
    predict_exact = user_dict[user]['recommends']

    compared = pd.DataFrame([target_bin,
                             user_dict[user]['preferred_binary'],
                             predict_exact], index=['fut', 'prev', 'predict_exact']).T
    compared.loc[:, 'predict_exact_new'] = np.where(compared.prev == 0, compared.predict_exact, 0)

    y_true = compared.sort_values('predict_exact_new', ascending=False).iloc[:k, :]['fut']
    y_pred = np.where(compared.sort_values('predict_exact_new', ascending=False).iloc[:k, :]['predict_exact_new'] > 0,
                      1, 0)
    acc = precision_score(y_true, y_pred)
    return acc


def get_metrics_at_k(user_item_diff, user_item_cut, user_dict, k):
    "считает для таргет матрицы и столбцов предиктов метрику на выбор"

    user_acc = {}
    for user in user_item_diff.index:
        precision = precision_at_k(user_item_diff, user_item_cut, user_dict, user, k)
        recall = recall_at_k(user_item_diff, user_dict, user, k)
        user_acc[user] = {}
        user_acc[user]['precision'] = precision
        user_acc[user]['recall'] = recall

    mean_precision = np.mean([user_acc[user]['precision'] for user in user_acc])
    max_precision = max([user_acc[user]['precision'] for user in user_acc])

    mean_recall = np.mean([user_acc[user]['recall'] for user in user_acc])
    max_recall = max([user_acc[user]['recall'] for user in user_acc])

    return mean_precision, max_precision, mean_recall, max_recall

