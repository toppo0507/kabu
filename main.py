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
    # output_dir = '/Users/あなたの名前/Desktop' # 必要に応じて変更

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
csv_file_name = f'Prime_Value_Stocks_{today_date_str}.csv'
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
roe_threshold = 10
per_threshold = 15
pbr_threshold = 1

def fetch_and_filter(ticker):
    try:
        stock = yf.Ticker(ticker)
        # サーバー負荷軽減のためランダム待機
        time.sleep(random.uniform(0.1, 0.5))
        info = stock.info
        
        roe = info.get("returnOnEquity", None)
        roe = roe * 100 if roe is not None else None
        per = info.get("trailingPE", None)
        pbr = info.get("priceToBook", None)

        if roe is None or per is None or pbr is None:
            return None

        # 基本的なフィルタリング
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

# --- 5. 結果整形・スコア計算・ソート ---
if not df_filtered.empty:
    result_df_key = df_filtered["Ticker"].astype(str).str.replace(".T", "", regex=False)
    merged_df = pd.merge(
        df_filtered,
        df_tickers[["コード", "銘柄名"]],
        left_on=result_df_key,
        right_on="コード",
        how="left"
    )
    
    # 【追加】Score計算: Score = ROE / (PER * PBR)
    # ゼロ除算回避のため、分母が0の場合はNaNなどにする処理を入れるのが安全ですが、
    # フィルタリングでPBR<1, PER<15としているため通常は非ゼロの正の値と仮定して計算します。
    merged_df["Score"] = merged_df["ROE"] / (merged_df["PER"] * merged_df["PBR"])
    
    # 【追加】スコアで見やすく丸める（小数点第2位まで）
    merged_df["Score"] = merged_df["Score"].round(2)
    merged_df["ROE"] = merged_df["ROE"].round(2)
    merged_df["PER"] = merged_df["PER"].round(2)
    merged_df["PBR"] = merged_df["PBR"].round(2)

    # 【追加】スコアの高い順（降順）にソート
    merged_df = merged_df.sort_values(by="Score", ascending=False)

    # カラムの並び順整理
    merged_df = merged_df[["Ticker", "銘柄名", "Score", "PBR", "PER", "ROE"]]
    
    merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"抽出数: {len(merged_df)}")
else:
    print("条件に合致する銘柄なし")
    merged_df = pd.DataFrame()

# --- 6. メール送信 ---
if GMAIL_USER and GMAIL_PASSWORD:
    print("メール送信準備中...")
    msg = MIMEMultipart()
    msg['Subject'] = f"【割安株】Score順レポート {today_date_str}"
    msg['From'] = GMAIL_USER
    msg['To'] = TO_EMAIL

    body = "本日のスクリーニング結果（Score降順 Top 10）です。\n"
    body += "Score = ROE / (PER * PBR)\n\n"
    
    if merged_df.empty:
        body += "該当銘柄はありませんでした。"
    else:
        # メール本文には上位10件を表示（見やすくするため）
        body += merged_df.head(10).to_string(index=False)
        body += "\n\n※全データは添付CSVを参照してください。"
    
    msg.attach(MIMEText(body, 'plain'))

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