import os

import pandas as pd
import yfinance as yf


class DownloadIndicators:
    def __init__(self):
        self.save_path = os.getenv("INDICATORS_SAVE_PATH")
        self.stock_ids = os.getenv("INDICATORS_STOCK_IDS", None).split(",")
        self.indicators = os.getenv("INDICATORS", None).split(",")

    def getIndicators(self):
        indicators_table = None
        for stock_id in self.stock_ids:
            ticker = yf.Ticker(stock_id + ".T")
            info = ticker.info

            indicators_data = {}
            indicators_data["NAME"] = [info.get("longName")]
            # PER(株価収益率)　株価/一株当たりの純利益
            if "PER" in self.indicators:
                indicators_data["tPER"] = [info.get("trailingPE")] # 実績PER
                indicators_data["fPER"] = [info.get("forwardPE")]  # 予想PER
            
            # (株価純資産倍率)　株価/一株当たりの純資産
            if "PBR" in self.indicators:
                indicators_data["PBR"] = [info.get("priceToBook")]
            
            # ROE(自己資本利益率)　当期純利益 / 自己資本比率
            if "ROE" in self.indicators:
                indicators_data["ROE"] = [info.get("returnOnEquity")]

            if indicators_table is None:
                indicators_table = pd.DataFrame(data=indicators_data)
            else:
                indicators_table = pd.concat([indicators_table,  pd.DataFrame(indicators_data)])
        indicators_table.to_csv(self.save_path, index=False)

    def main(self):
        self.getIndicators()