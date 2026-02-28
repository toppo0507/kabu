import os
import itertools
import numpy as np
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import japanize_matplotlib 

industry_colors = {
    # エネルギー・資源（暖色・重厚な色）
    '石油': 'firebrick',
    'ガス': 'darkorange',
    '電力': 'gold',
    '鉱業': 'sienna',
    
    # 素材・製造（無機質・力強い色）
    '鉄鋼': 'slategray',
    '非鉄・金属': 'darkgray',
    '金属': 'grey',
    '化学': 'darkcyan',
    'ゴム': 'darkviolet',
    '窯業': 'peru',
    'パルプ・紙': 'antiquewhite',
    '繊維': 'thistle',
    
    # 機械・テクノロジー（青・クールな色）
    '機械': 'steelblue',
    '電気機器': 'dodgerblue',
    '精密機器': 'cadetblue',
    '自動車': 'midnightblue',
    '造船': 'teal',
    
    # 金融・不動産（信頼感のある濃紺・落ち着いた色）
    '銀行': 'navy',
    '証券': 'royalblue',
    '保険': 'mediumblue',
    'その他金融': 'cornflowerblue',
    '不動産': 'chocolate',
    
    # インフラ・流通（動きを感じる色）
    '鉄道・バス': 'darkgreen',
    '陸運': 'forestgreen',
    '海運': 'darkslateblue',
    '空運': 'skyblue',
    '通信': 'mediumslateblue',
    '情報・通信業': 'blueviolet',
    
    # 生活・サービス（明るい・柔らかな色）
    '食品': 'limegreen',
    '水産': 'lightseagreen',
    '建設': 'goldenrod',
    '小売業': 'indianred',
    '商社': 'darkkhaki',
    '医薬品': 'hotpink',
    'サービス': 'mediumorchid',
    
    # その他
    'その他製造': 'gray',
    'その他製品': 'silver'
}

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
    
    def scatter_by_industory(self):
        data_array = self.row_data.to_numpy()
        commpany_num = data_array.shape[0]
        item_list = [i for i in range(commpany_num)]
        p = itertools.permutations(item_list, 2)
        scatter_dict = {}
        for v in p:
            i = v[0]
            j = v[1]
            industry_i = self.id_name_industry.iloc[i]
            industry_j = self.id_name_industry.iloc[j]
            if industry_i["industry"] != industry_j["industry"]:
                continue
            industry_now = industry_i["industry"]
            if industry_now in scatter_dict:
                scatter_dict[industry_now]["xlist"].append(data_array[i, j])
                scatter_dict[industry_now]["ylist"].append(data_array[j, i])
                scatter_dict[industry_now]["clist"].append(industry_colors[industry_now])
            else:
                scatter_dict[industry_now] = {"xlist":[data_array[i, j]], "ylist":[data_array[j, i]], "clist":[industry_colors[industry_now]],}
        
        plt.figure(figsize=(30, 30))
        for industry_i in scatter_dict:
            if industry_i not in ['機械',
    '電気機器',
    '精密機器',
    '自動車',
    '造船']:
                continue
            plt.scatter(scatter_dict[industry_i]["xlist"], scatter_dict[industry_i]["ylist"], c=scatter_dict[industry_i]["clist"], label=industry_i)
        plt.title("chatterjee Matrix")
        plt.legend()
        plt.grid()
        plt.savefig(self.save_path)

    def main(self):
        # self.scatter_by_industory()
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
