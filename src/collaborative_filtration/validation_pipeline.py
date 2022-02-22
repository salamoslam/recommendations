import sys
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.insert(1, '../src/data')
import pandas as pd
from sklearn.metrics import precision_score
import numpy as np
from nearest_neighbours import *
from metrics import *

def get_validation_plots(user_item_cut, user_item_old_cut, user_item_diff, k_s = [5, 10, 20]):

    results_euclid = pd.DataFrame(columns=[i for sub in [(f'euclid_mean_{metrics}_{k_s[j]}',
                                                          f'euclid_max_{metrics}_{k_s[j]}') for j in range(len(k_s))
                                                         for metrics in ['precision', 'recall']] for i in sub])
    results_cosine = pd.DataFrame(columns=[i for sub in [(f'cosine_mean_{metrics}_{k_s[j]}',
                                                          f'cosine_max_{metrics}_{k_s[j]}') for j in range(len(k_s))
                                                         for metrics in ['precision', 'recall']] for i in sub])
    for metric in ['euclid', 'cosine']:
        for k in tqdm(range(5, 20), leave=True):
            user_dict = recommend_NN(user_item_old_cut, user_item_diff.index, metric=metric, k=k)
            if metric == 'euclid':
                a = get_metrics_at_k(user_item_diff, user_item_cut, user_dict, k_s[0]), \
                    get_metrics_at_k(user_item_diff, user_item_cut, user_dict, k_s[1]), \
                    get_metrics_at_k(user_item_diff, user_item_cut, user_dict, k_s[2])

                results_euclid.loc[k] = [i for sub in a for i in sub]
            if metric == 'cosine':
                a = get_metrics_at_k(user_item_diff, user_item_cut, user_dict, k_s[0]), \
                    get_metrics_at_k(user_item_diff, user_item_cut, user_dict, k_s[1]), \
                    get_metrics_at_k(user_item_diff, user_item_cut, user_dict, k_s[2])

                results_cosine.loc[k] = [i for sub in a for i in sub]
    results1 = results_euclid.join(results_cosine)
    return results1.plot(subplots=True, figsize=(20, 50))