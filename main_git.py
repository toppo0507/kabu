import os
import datetime
import pandas as pd
import yfinance as yf
import time
import concurrent.futures
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from tqdm import tqdm  # notebook用ではなく標準のtqdmに変更

# --- 設定：環境変数から読み込む ---
# GitHubのSettings > Secretsで設定した値がここに入ります
GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
TO_EMAIL = GMAIL_USER  # 自分宛て

# --- 1. 日付・保存先設定 ---
def get_today_yyyymmdd():
    today = datetime.date.today()
    return today.strftime('%Y%m%d')
today_date_str = get_today_yyyymmdd()

# GitHub Actions上の一時保存先（カレントディレクトリ）
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
    exit() # エラーなら終了

# --- 3. データ取得・フィルタリング関数 ---
roe_threshold = 10
per_threshold = 15
pbr_threshold = 1

def fetch_and_filter(ticker):
    try:
        stock = yf.Ticker(ticker)
        # info取得は通信が発生するため、少し待機を入れるか、エラーハンドリングを強化
        info = stock.info
        
        roe = info.get("returnOnEquity", None)
        roe = roe * 100 if roe is not None else None
        per = info.get("trailingPE", None)
        pbr = info.get("priceToBook", None)

        if roe is None or per is None or pbr is None:
            return None

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

# GitHub Actionsのスペックに合わせてワーカー数を調整
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_and_filter, ticker) for ticker in ticker_list]
    for future in tqdm(concurrent.futures.as_completed(futures), total=len(ticker_list)):
        result = future.result()
        if result:
            filtered_stocks.append(result)

df_filtered = pd.DataFrame(filtered_stocks)

# --- 5. 結果整形とCSV保存 ---
if not df_filtered.empty:
    result_df_key = df_filtered["Ticker"].astype(str).str.replace(".T", "", regex=False)
    merged_df = pd.merge(
        df_filtered,
        df_tickers[["コード", "銘柄名"]],
        left_on=result_df_key,
        right_on="コード",
        how="left"
    )
    merged_df = merged_df[["Ticker", "銘柄名", "PBR", "PER", "ROE"]]
    merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"抽出数: {len(merged_df)}")
else:
    print("条件に合致する銘柄なし")
    merged_df = pd.DataFrame()

# --- 6. メール送信 ---
if GMAIL_USER and GMAIL_PASSWORD:
    print("メール送信準備中...")
    msg = MIMEMultipart()
    msg['Subject'] = f"【株価スクリーニング】{today_date_str}"
    msg['From'] = GMAIL_USER
    msg['To'] = TO_EMAIL

    body = "本日のスクリーニング結果です。\n\n"
    if merged_df.empty:
        body += "該当銘柄はありませんでした。"
    else:
        body += merged_df.head(5).to_string(index=False)
        body += "\n\n※全データは添付CSVを参照"
    
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