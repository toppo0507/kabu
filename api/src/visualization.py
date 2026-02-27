import os
import itertools

import numpy as np
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import japanize_matplotlib 



class Visualization:
    def __init__(self):
        self.row_data = pd.read_csv(os.getenv("VISUALIZATION_DATA_PATH"), header=0, index_col=0)
        self.save_path = os.getenv("VISUALIZATION_SAVE_PATH")
        self.id_name_industry = pd.read_csv(os.getenv("NIKKEI_SAVE_PATH"), index_col=0, header=0)

    def bias(self, data):
        threshold=0.95
        data = data.mask((-1*threshold <= data) & (data <= threshold), 0)
        data = data.mask((data < -1*threshold) & (threshold < data), 1)
        np.fill_diagonal(data.values, 0)
        return data

    def scatter(self):
        data_array = self.row_data.to_numpy()
        commpany_num = data_array.shape[0]
        item_list = [i for i in range(commpany_num)]
        p = itertools.permutations(item_list, 2)
        x_list = []
        y_list = []
        color_list = []
        for v in p:
            i = v[0]
            j = v[1]
            industry_i = self.id_name_industry.iloc[i]
            industry_j = self.id_name_industry.iloc[j]
            color = "b" if industry_i["industry"] == industry_j["industry"] else "r"
            x_list.append(data_array[i, j])
            y_list.append(data_array[j, i])
            color_list.append(color)
        xlist = []
        ylist = []
        clist = []
        for i in range(len(color_list)):
            if color_list[i] == "b":
                xlist.append(x_list[i])
                ylist.append(y_list[i])
                clist.append(color_list[i])
            else:
                xlist.insert(0, x_list[i])
                ylist.insert(0, y_list[i])
                clist.insert(0, color_list[i])


        plt.figure(figsize=(30, 30))
        plt.scatter(xlist, ylist, c=clist)
        plt.title("chatterjee Matrix")
        plt.savefig(self.save_path)

    def main(self):
        self.scatter()
        # data = self.bias(self.row_data)

        # G = nx.from_pandas_adjacency(data)
        # G.remove_edges_from(nx.selfloop_edges(G))
        # plt.figure(figsize=(50, 50))
        # nx.draw(G, pos=nx.circular_layout(G), with_labels=True, node_color='orange', node_size=1000, font_weight='bold',font_family="IPAexGothic")

        # plt.title("Graph from Adjacency Matrix")
        # plt.savefig(self.save_path)


def main():
    pass

if __name__ == "__main__":
    main()