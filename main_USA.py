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

# --- 0. 設定・定数定義 ---

# ★ スクリーニング基準値の設定（ここを変更すれば調整可能）
CONFIG = {
    "JP": {
        "ROE": 10,    # 日本株: ROE 10%以上
        "PER": 15,    # 日本株: PER 15倍以下
        "PBR": 1.2,   # 日本株: PBR 1.2倍以下 (1倍割れ狙いなら1.0へ)
        "MAX_WORKERS": 10
    },
    "US": {
        "ROE": 15,    # 米国株: ROE 15%以上 (高収益体質)
        "PER": 20,    # 米国株: PER 20倍以下 (成長期待込み)
        "PBR": 3.0,   # 米国株: PBR 3倍以下
        "MAX_WORKERS": 20
    }
}

# 環境設定
is_github = os.getenv("GITHUB_ACTIONS") == "true"
if is_github:
    output_dir = '.'
    GMAIL_USER = os.environ.get("GMAIL_USER")
    GMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
    TO_EMAIL = GMAIL_USER
else:
    load_dotenv()
    GMAIL_USER = os.getenv("GMAIL_USER")
    GMAIL_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
    TO_EMAIL = GMAIL_USER
    output_dir = '.' # 必要に応じて変更

today_str = datetime.date.today().strftime('%Y%m%d')

# --- 1. 共通関数: データ取得とフィルタリング ---

def fetch_stock_data(ticker, config):
    """指定された銘柄のデータを取得し、条件に合えば辞書を返す"""
    try:
        stock = yf.Ticker(ticker)
        time.sleep(random.uniform(0.1, 0.5)) # 負荷軽減
        
        try:
            info = stock.info
        except:
            return None

        # データ抽出
        roe = info.get("returnOnEquity", None)
        roe = roe * 100 if roe is not None else None
        per = info.get("trailingPE", None)
        pbr = info.get("priceToBook", None)
        name = info.get("shortName", ticker) # 英語名または社名
        sector = info.get("sector", "-")

        if roe is None or per is None or pbr is None:
            return None

        # フィルタリング判定
        if (roe > config["ROE"] and 
            per < config["PER"] and 
            pbr < config["PBR"]):
            
            return {
                "Ticker": ticker,
                "Name": name,
                "Score": 0, # 後で計算
                "PBR": pbr,
                "PER": per,
                "ROE": roe,
                "Sector": sector
            }
    except Exception:
        return None
    return None

def run_screening(ticker_list, region_key):
    """銘柄リストを受け取り、並列処理でスクリーニングを行う"""
    config = CONFIG[region_key]
    results = []
    
    print(f"[{region_key}] スクリーニング開始: 対象 {len(ticker_list)} 銘柄 (基準: ROE>{config['ROE']}%, PER<{config['PER']}x, PBR<{config['PBR']}x)")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=config["MAX_WORKERS"]) as executor:
        futures = [executor.submit(fetch_stock_data, t, config) for t in ticker_list]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(ticker_list)):
            res = future.result()
            if res:
                results.append(res)
    
    df = pd.DataFrame(results)
    
    if not df.empty:
        # Score計算: Score = ROE / (PER * PBR)
        df["Score"] = df["ROE"] / (df["PER"] * df["PBR"])
        
        # 丸め処理
        cols = ["Score", "ROE", "PER", "PBR"]
        df[cols] = df[cols].round(2)
        
        # ソートとカラム整理
        df = df.sort_values(by="Score", ascending=False)
        output_cols = ["Ticker", "Name", "Score", "PBR", "PER", "ROE", "Sector"]
        df = df[output_cols]
    
    return df

# --- 2. 銘柄リスト取得関数 ---

def get_jp_tickers():
    print("JPXから日本株リストを取得中...")
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    try:
        df = pd.read_excel(url, sheet_name=0, engine="xlrd") # xlrdが必要です
        target = ["プライム（内国株式）", "スタンダード（内国株式）", "グロース（内国株式）"]
        df = df[df["市場・商品区分"].isin(target)]
        # 4桁コード + .T
        tickers = df["コード"].astype(str).str.zfill(4) + ".T"
        return tickers.tolist()
    except Exception as e:
        print(f"日本株リスト取得エラー: {e}")
        return []

def get_us_tickers():
    print("Wikipediaから米国株(S&P500)リストを取得中...")
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    try:
        tables = pd.read_html(url) # lxml, html5libが必要です
        df = tables[0]
        # シンボルの . を - に変換 (例: BRK.B -> BRK-B)
        tickers = df['Symbol'].str.replace('.', '-', regex=False)
        return tickers.tolist()
    except Exception as e:
        print(f"米国株リスト取得エラー: {e}")
        return []

# --- 3. メイン処理実行 ---

# 日本株実行
jp_tickers = get_jp_tickers()
df_jp = run_screening(jp_tickers, "JP")
csv_jp = os.path.join(output_dir, f'JP_Value_Stocks_{today_str}.csv')
if not df_jp.empty:
    df_jp.to_csv(csv_jp, index=False, encoding='utf-8-sig')
    print(f"日本株 抽出数: {len(df_jp)}")

# 米国株実行
us_tickers = get_us_tickers()
df_us = run_screening(us_tickers, "US")
csv_us = os.path.join(output_dir, f'US_Value_Stocks_{today_str}.csv')
if not df_us.empty:
    df_us.to_csv(csv_us, index=False, encoding='utf-8-sig')
    print(f"米国株 抽出数: {len(df_us)}")

# --- 4. メール送信 (統合版) ---

if GMAIL_USER and GMAIL_PASSWORD:
    print("メール作成中...")
    msg = MIMEMultipart()
    msg['Subject'] = f"【日米合同】厳選バリュー株レポート {today_str}"
    msg['From'] = GMAIL_USER
    msg['To'] = TO_EMAIL

    # HTML本文作成関数
    def create_html_table(df, title, color):
        if df.empty:
            return f"<h3>{title}</h3><p>該当なし</p>"
        return f"""
        <h3>{title} (Top 10)</h3>
        {df.head(10).to_html(index=False, border=1, justify='center', classes='stock_table')}
        """

    # スタイル定義
    style = """
    <style>
        table { border-collapse: collapse; width: 100%; max-width: 800px; margin-bottom: 20px; }
        th, td { border: 1px solid #ddd; text-align: center; padding: 6px; font-size: 13px; }
        th { background-color: #f2f2f2; }
    </style>
    """
    
    body_jp = create_html_table(df_jp, "🇯🇵 日本株 (JPX)", "#cc0000")
    body_us = create_html_table(df_us, "🇺🇸 米国株 (S&P500)", "#003366")
    
    html_content = f"""
    <html>
    <head>{style}</head>
    <body>
        <h2>本日のバリュー株スクリーニング結果</h2>
        <p>Score = ROE / (PER * PBR)</p>
        <hr>
        {body_jp}
        <hr>
        {body_us}
        <br>
        <p>※詳細は添付のCSVファイルをご確認ください。</p>
    </body>
    </html>
    """
    
    msg.attach(MIMEText(html_content, 'html'))

    # CSV添付 (存在するファイルのみ)
    for path, name in [(csv_jp, f"JP_Stocks_{today_str}.csv"), (csv_us, f"US_Stocks_{today_str}.csv")]:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path, "rb") as f:
                part = MIMEApplication(f.read(), Name=name)
            part['Content-Disposition'] = f'attachment; filename="{name}"'
            msg.attach(part)

    # 送信
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
    print("メール設定なしのため送信スキップ")