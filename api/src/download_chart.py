import os
import requests

import yfinance as yf
from bs4 import BeautifulSoup
import pandas as pd


class DownloadDataTable:
    def __init__(self):
        self.stock_id = os.getenv("STOCK_ID", None)
        self.start_date = os.getenv("START_DATE")
        self.end_date = os.getenv("END_DATE")
        self.history_interval=os.getenv("INTERVAL")
        self.id_table = pd.read_csv(os.getenv("NIKKEI_SAVE_PATH"))
    
    """
    企業コードからチャート一覧をダウンロードする
    """
    def getStockChartFromID(self, stock_id):
        if self.stock_id is not None:
            STOCK_dividends = yf.download(self.stock_id, start=self.start_date, end=self.end_date, interval=self.history_interval)
        else:
            STOCK_dividends = yf.download(stock_id, start=self.start_date, end=self.end_date, interval=self.history_interval)
        STOCK_dividends = STOCK_dividends[os.getenv("DATA_TYPE").split(",")]
        return STOCK_dividends

    """
    企業コード一覧からチャートをまとめてダウンロードする
    """
    def downloadChartsFromIndex(self):
        stock_chart = None
        id_list = self.id_table[["id", "name", "industry"]].to_numpy()
        for idx, id_name_industry in enumerate(id_list):
            stock_id = f"{id_name_industry[0]}.T"
            company_name = f"{id_name_industry[1]}"
            industry = f"{id_name_industry[2]}"
            print(f"{stock_id} : {company_name} : {industry}")
            chart = self.getStockChartFromID(stock_id).rename({stock_id: company_name}, axis='columns')
            if stock_chart is None:
                stock_chart = chart
            else:
                stock_chart = pd.merge(stock_chart, chart, how="inner", on = "Date")
            
        stock_chart.to_csv(os.getenv("CHART_SAVE_PATH"))


    """
    日経２２５の構成銘柄に選択された企業の企業IDと企業名と業界をダウンロードする
    """
    def downloadNikkei225Ids(self):
        url = "https://www.sbisec.co.jp/ETGate/WPLETmgR001Control?OutSide=on&getFlg=on&burl=search_market&cat1=market&cat2=info&dir=info&file=market_meigara_225.html"
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find('table', {'class':'md-l-table-01 md-l-utl-mt10'}).tbody

        stock_list=[]
        for tr in table.find_all('tr'):
            cols = tr.find_all('td')
    
            # tdが3つある場合のみ処理（ヘッダーなどを除外するため）
            if len(cols) == 3:
                code = cols[0].get_text(strip=True)     # 9983
                name = cols[1].get_text(strip=True)     # ファーストリテイリング
                industry = cols[2].get_text(strip=True) # 小売業
                
                # 辞書に格納
                stock_data = {
                    "id": code,
                    "name": name,
                    "industry": industry
                }
                stock_list.append(stock_data)
        stock_table = pd.DataFrame(stock_list)
        stock_table.to_csv(os.getenv("NIKKEI_SAVE_PATH"), index=False)

    def main(self):
        print("=== 日経平均構成銘柄の一覧をダウンロード ===")
        self.downloadNikkei225Ids()
        print("=== 日経平均構成銘柄の一覧をダウンロード完了 ===")

        print("=== 日経平均構成銘柄のチャートを一括ダウンロード")
        self.downloadChartsFromIndex()



def main():
    print("ここが実行されるよ")


if __name__ == "__main__":
    main()
