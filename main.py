import os
import datetime
import pandas as pd
import yfinance as yf
import time
import random
import concurrent.futures
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from tqdm import tqdm
from dotenv import load_dotenv

# --- 設定：環境変数から読み込む ---

# GitHub Actions 上で実行されているかチェック
is_github = os.getenv("GITHUB_ACTIONS") == "true"

if is_github:
    print("【モード】GitHub Actions で実行中")
    output_dir = '.' 
    GMAIL_USER = os.environ.get("GMAIL_USER")
    GMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
    TO_EMAIL = GMAIL_USER
else:
    print("【モード】ローカル環境 で実行中")
    load_dotenv()
    GMAIL_USER = os.getenv("GMAIL_USER")
    GMAIL_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
    TO_EMAIL = GMAIL_USER

if GMAIL_USER:
    print(f"ユーザー設定OK: {GMAIL_USER}")
else:
    print("ユーザー設定NG: 環境変数が読み込めていません")

# --- 1. 日付・保存先設定 ---
def get_today_yyyymmdd():
    today = datetime.date.today()
    return today.strftime('%Y%m%d')
today_date_str = get_today_yyyymmdd()

output_dir = '.'
csv_file_name = f'Magic_Formula_Stocks_{today_date_str}.csv' # ファイル名を変更
output_path = os.path.join(output_dir, csv_file_name)

# --- 2. JPXから銘柄リストを取得 ---
url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
print("JPX公式サイトから銘柄データを取得中...")

try:
    df_tickers = pd.read_excel(url, sheet_name=0, engine="xlrd")
    target_markets = ["プライム（内国株式）", "スタンダード（内国株式）", "グロース（内国株式）"]
    df_tickers = df_tickers[df_tickers["市場・商品区分"].isin(target_markets)].copy()
    df_tickers["コード"] = df_tickers["コード"].astype(str).str.zfill(4)
    ticker_list = df_tickers["コード"].astype(str) + ".T"
    print(f"対象銘柄数: {len(ticker_list)}")
except Exception as e:
    print(f"銘柄リスト取得エラー: {e}")
    exit()

# --- 3. データ取得・フィルタリング関数 ---
# ※ここで極端な変な銘柄を弾くために、緩めの閾値で一次選抜します
roe_threshold = 8      # ROE 8%以上（少し緩めて広く拾う）
per_threshold = 25     # PER 25倍以下（高すぎるものを排除）
pbr_threshold = 1.5    # PBR 1.5倍以下

def fetch_and_filter(ticker):
    try:
        stock = yf.Ticker(ticker)
        # サーバー負荷軽減
        time.sleep(random.uniform(0.1, 0.5))
        info = stock.info
        
        roe = info.get("returnOnEquity", None)
        roe = roe * 100 if roe is not None else None
        per = info.get("trailingPE", None)
        pbr = info.get("priceToBook", None)

        if roe is None or per is None or pbr is None:
            return None

        # 一次フィルタリング（閾値に入らないものは計算対象外にする）
        if roe > roe_threshold and per < per_threshold and pbr < pbr_threshold:
            return {
                "Ticker": ticker,
                "PBR": pbr,
                "PER": per,
                "ROE": roe
            }
    except Exception:
        return None
    return None

# --- 4. 並列処理実行 ---
filtered_stocks = []
print("スクリーニングを開始します...")

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_and_filter, ticker) for ticker in ticker_list]
    for future in tqdm(concurrent.futures.as_completed(futures), total=len(ticker_list)):
        result = future.result()
        if result:
            filtered_stocks.append(result)

df_filtered = pd.DataFrame(filtered_stocks)

# --- 5. 結果整形・新公式適用・ソート ---
if not df_filtered.empty:
    result_df_key = df_filtered["Ticker"].astype(str).str.replace(".T", "", regex=False)
    merged_df = pd.merge(
        df_filtered,
        df_tickers[["コード", "銘柄名"]],
        left_on=result_df_key,
        right_on="コード",
        how="left"
    )
    
    # === 新しい計算ロジック ===
    
    # 1. ミックス係数 (Graham's Mix) = PER * PBR
    merged_df["Mix_Coeff"] = merged_df["PER"] * merged_df["PBR"]
    
    # 2. 魔法の公式風スコアリング (Magic Formula Style)
    # ROEの順位（高いほうが良い -> ascending=False）
    merged_df["Rank_ROE"] = merged_df["ROE"].rank(ascending=False)
    # PERの順位（低いほうが良い -> ascending=True）
    merged_df["Rank_PER"] = merged_df["PER"].rank(ascending=True)
    
    # 総合スコア = ROE順位 + PER順位 （低いほうが優秀）
    merged_df["Magic_Score"] = merged_df["Rank_ROE"] + merged_df["Rank_PER"]
    
    # 見やすく丸める
    merged_df["Mix_Coeff"] = merged_df["Mix_Coeff"].round(2)
    merged_df["ROE"] = merged_df["ROE"].round(2)
    merged_df["PER"] = merged_df["PER"].round(2)
    merged_df["PBR"] = merged_df["PBR"].round(2)

    # Magic_Scoreが小さい順（成績が良い順）にソート
    merged_df = merged_df.sort_values(by="Magic_Score", ascending=True)

    # カラム整理（見たい指標を前に）
    output_columns = ["Ticker", "銘柄名", "Magic_Score", "Mix_Coeff", "ROE", "PER", "PBR"]
    merged_df = merged_df[output_columns]
    
    merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"抽出数: {len(merged_df)}")
else:
    print("条件に合致する銘柄なし")
    merged_df = pd.DataFrame()

# --- 6. メール送信 ---
if GMAIL_USER and GMAIL_PASSWORD:
    print("メール送信準備中...")
    msg = MIMEMultipart()
    # 件名変更
    msg['Subject'] = f"【魔法の公式】厳選割安株レポート {today_date_str}"
    msg['From'] = GMAIL_USER
    msg['To'] = TO_EMAIL

    if merged_df.empty:
        html_content = "<p>該当銘柄はありませんでした。</p>"
    else:
        # 上位15件を表示
        top_stocks_html = merged_df.head(15).to_html(
            index=False, 
            border=1, 
            justify='center',
            classes='stock_table'
        )
        
        html_content = f"""
        <html>
        <head>
        <style>
            table {{ border-collapse: collapse; width: 100%; max-width: 800px; font-family: sans-serif; }}
            th, td {{ border: 1px solid #ddd; text-align: center; padding: 8px; font-size: 14px; }}
            th {{ background-color: #4CAF50; color: white; }}
            tr:nth-child(even) {{ background-color: #f2f2f2; }}
            tr:hover {{ background-color: #ddd; }}
        </style>
        </head>
        <body>
            <h3>本日の優良割安株（Magic Score順 Top 15）</h3>
            <ul>
                <li><b>Magic_Score:</b> ROE順位 + PER順位（低いほど良い）</li>
                <li><b>Mix_Coeff:</b> PER × PBR（グレアムの指標、22.5以下が割安）</li>
            </ul>
            {top_stocks_html}
            <br>
            <p>※全データは添付CSVを参照してください。</p>
        </body>
        </html>
        """

    msg.attach(MIMEText(html_content, 'html'))

    if not merged_df.empty and os.path.exists(output_path):
        with open(output_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=csv_file_name)
        part['Content-Disposition'] = f'attachment; filename="{csv_file_name}"'
        msg.attach(part)

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(GMAIL_USER, GMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        print("メール送信完了")
    except Exception as e:
        print(f"メール送信エラー: {e}")
else:
    print("メール設定がないため送信をスキップしました")