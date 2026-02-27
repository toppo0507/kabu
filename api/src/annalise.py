import os
import itertools

import numpy as np
import pandas as pd
from scipy.stats import rankdata


def chatterjee_correlation(x, y):
    """
    Chatterjeeの順位相関係数 (ξn) を計算する関数
    """
    x = np.asarray(x)
    y = np.asarray(y)
    n = len(x)
    
    # 1. Xの昇順にデータをソートする
    # argsortを使ってインデックスを取得し、Yを並べ替える
    sort_idx = np.argsort(x)
    y_sorted = y[sort_idx]
    
    # 2. Yの順位(ランク)を計算する
    # rankdataはデフォルトで平均順位を返すが、ここでは単純化のためordinalを使う
    # (厳密にはタイの処理が必要だが、概念理解のため簡略化)
    r = rankdata(y_sorted, method='ordinal')
    
    # 3. 隣り合うランクの差の絶対値の総和を計算
    diff_sum = np.sum(np.abs(np.diff(r)))
    
    # 4. 公式に当てはめる
    xi = 1 - (3 * diff_sum) / (n**2 - 1)
    
    return xi


class Annalise:
    def __init__(self):
        self.chart_log = pd.read_csv(os.getenv("ANNALIZED_DATA_PATH"), skiprows=[0,2], index_col=0, header=0)
        self.date_list = self.chart_log.index
        self.commpany_name_list = self.chart_log.columns

    # 相関係数
    def getCorrelationCoefficient(self):
        return self.chart_log.corr()

    # 新しい相関係数　https://qiita.com/Islay_tr/items/dd427ba86ba11bd25626
    def Chatterjee(self):
        commpany_num = len(self.chart_log.columns)
        item_list = [i for i in range(commpany_num)]
        p = itertools.permutations(item_list, 2)
        chart_arry = self.chart_log.to_numpy()
        Chatterjee_matrix = np.eye(commpany_num)
        for v in p:
            chart_x = chart_arry[:,v[0]]
            chart_y = chart_arry[:,v[1]]
            Chatterjee_matrix[v[0], v[1]] = chatterjee_correlation(chart_x, chart_y)
        Chatterjee_matrix = pd.DataFrame(Chatterjee_matrix, index=self.chart_log.columns, columns=self.chart_log.columns)
        return Chatterjee_matrix

    def main(self):
        # corr = self.getCorrelationCoefficient()
        # corr.index = corr.columns
        # corr.to_csv(os.getenv("ANNALIZED_SAVE_PATH"))
        Chatterjee_corr = self.Chatterjee()
        Chatterjee_corr.to_csv(os.getenv("ANNALIZED_CHATTERJEE_SAVE_PATH"))
        

def main():
    pass

if __name__ == "__main__":
    main()