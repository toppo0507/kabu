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

# --- 0. 環境設定 ---
# GitHub Actions判定
is_github = os.getenv("GITHUB_ACTIONS") == "true"

if is_github:
    print("【モード】GitHub Actions")
    output_dir = '.' 
    GMAIL_USER = os.environ.get("GMAIL_USER")
    GMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
    TO_EMAIL = GMAIL_USER
else:
    print("【モード】ローカル環境")
    load_dotenv()
    GMAIL_USER = os.getenv("GMAIL_USER")
    GMAIL_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
    TO_EMAIL = GMAIL_USER

def get_today_yyyymmdd():
    return datetime.date.today().strftime('%Y%m%d')

today_str = get_today_yyyymmdd()

# --- 1. 共通関数：データ取得ワーカー ---
def fetch_stock_data(args):
    """
    並列処理用のワーカー関数
    args: (ticker, thresholds_dict)
    """
    ticker, th = args
    try:
        stock = yf.Ticker(ticker)
        time.sleep(random.uniform(0.1, 0.5)) # 負荷分散
        info = stock.info
        
        roe = info.get("returnOnEquity", None)
        roe = roe * 100 if roe is not None else None
        per = info.get("trailingPE", None)
        pbr = info.get("priceToBook", None)

        if roe is None or per is None or pbr is None:
            return None

        # 閾値判定
        if (roe > th['roe_min'] and 
            per < th['per_max'] and 
            pbr < th['pbr_max']):
            return {
                "Ticker": ticker,
                "PBR": pbr,
                "PER": per,
                "ROE": roe
            }
    except Exception:
        return None
    return None

# --- 2. 共通関数：市場ごとの分析実行 ---
def analyze_market(market_name, ticker_list, name_map, thresholds):
    """
    指定された市場（日本/米国）のリストに対してスクリーニングを実行し、
    結果のDataFrameと保存パスを返す
    """
    print(f"\n--- {market_name} の分析を開始 ---")
    print(f"対象銘柄数: {len(ticker_list)}")
    print(f"閾値: ROE>{thresholds['roe_min']}%, PER<{thresholds['per_max']}倍, PBR<{thresholds['pbr_max']}倍")

    results = []
    # 引数をタプルにまとめる
    worker_args = [(t, thresholds) for t in ticker_list]

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # tqdmで進捗表示
        futures = [executor.submit(fetch_stock_data, arg) for arg in worker_args]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            res = future.result()
            if res:
                results.append(res)

    if not results:
        print(f"{market_name}: 該当なし")
        return pd.DataFrame(), None

    df = pd.DataFrame(results)

    # 銘柄名のマッピング
    if name_map:
        df["銘柄名"] = df["Ticker"].map(name_map)
        df["銘柄名"] = df["銘柄名"].fillna(df["Ticker"]) # マップになければTickerを入れる
    
    # === Magic Score 計算 ===
    # 1. Mix係数
    df["Mix_Coeff"] = df["PER"] * df["PBR"]
    
    # 2. ランク付け (ROEは高い順、PERは低い順)
    df["Rank_ROE"] = df["ROE"].rank(ascending=False)
    df["Rank_PER"] = df["PER"].rank(ascending=True)
    
    # 3. スコア合算 (低いほうが良い)
    df["Magic_Score"] = df["Rank_ROE"] + df["Rank_PER"]

    # 丸め処理
    cols_to_round = ["Mix_Coeff", "ROE", "PER", "PBR"]
    df[cols_to_round] = df[cols_to_round].round(2)

    # ソート
    df = df.sort_values(by="Magic_Score", ascending=True)
    
    # カラム整理
    output_cols = ["Ticker", "銘柄名", "Magic_Score", "Mix_Coeff", "ROE", "PER", "PBR"]
    # 銘柄名がない場合（name_mapがNoneの場合など）を考慮
    final_cols = [c for c in output_cols if c in df.columns]
    df = df[final_cols]

    # CSV保存
    filename = f"{market_name}_MagicStocks_{today_str}.csv"
    path = os.path.join(output_dir, filename)
    df.to_csv(path, index=False, encoding='utf-8-sig')
    print(f"{market_name} 抽出数: {len(df)}")
    
    return df, path

# --- 3. メイン処理 ---

# === A. 日本株リスト取得 ===
print("\n[Data Fetch] JPXリスト取得中...")
try:
    url_jpx = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    df_jpx = pd.read_excel(url_jpx, sheet_name=0, engine="xlrd")
    target_markets = ["プライム（内国株式）", "スタンダード（内国株式）"] # グロースは除外推奨（データ欠損が多いため）
    df_jpx = df_jpx[df_jpx["市場・商品区分"].isin(target_markets)].copy()
    df_jpx["コード"] = df_jpx["コード"].astype(str).str.zfill(4)
    
    jp_tickers = (df_jpx["コード"] + ".T").tolist()
    # 名前マップ作成 (key: 1234.T, value: トヨタ)
    jp_name_map = dict(zip(jp_tickers, df_jpx["銘柄名"]))
except Exception as e:
    print(f"JPX取得エラー: {e}")
    jp_tickers = []
    jp_name_map = {}

# === B. 米国株リスト取得 (S&P 500) ===
print("[Data Fetch] S&P 500リスト取得中...")
try:
    url_sp500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url_sp500)
    df_sp500 = tables[0]
    # ドットをハイフンに変換 (BRK.B -> BRK-B)
    us_tickers = df_sp500["Symbol"].apply(lambda x: x.replace(".", "-")).tolist()
    us_name_map = dict(zip(us_tickers, df_sp500["Security"]))
except Exception as e:
    print(f"S&P 500取得エラー: {e}")
    us_tickers = []
    us_name_map = {}

# === C. 分析実行 ===

# 日本株の設定（厳しめ）
jp_thresholds = {'roe_min': 8, 'per_max': 25, 'pbr_max': 1.5}
df_jp, path_jp = analyze_market("Japan", jp_tickers, jp_name_map, jp_thresholds)

# 米国株の設定（少し緩め）
us_thresholds = {'roe_min': 15, 'per_max': 35, 'pbr_max': 5.0}
df_us, path_us = analyze_market("US", us_tickers, us_name_map, us_thresholds)


# --- 4. メール作成・送信 ---
if GMAIL_USER and GMAIL_PASSWORD:
    print("\nメール作成中...")
    msg = MIMEMultipart()
    msg['Subject'] = f"【日米合同】厳選割安株レポート {today_str}"
    msg['From'] = GMAIL_USER
    msg['To'] = TO_EMAIL

    # HTML本文の作成
    def create_html_table(df, title):
        if df.empty:
            return f"<h4>{title}</h4><p>該当なし</p>"
        # Top 10のみ表示
        return f"""
        <h4>{title} (Top 10)</h4>
        {df.head(10).to_html(index=False, border=1, justify='center')}
        """

    html_jp = create_html_table(df_jp, "🇯🇵 日本株 (Low PBR Focus)")
    html_us = create_html_table(df_us, "🇺🇸 米国株 (High Quality Focus)")

    full_html = f"""
    <html>
    <head>
    <style>
        table {{ border-collapse: collapse; width: 100%; max-width: 600px; font-size: 12px; }}
        th, td {{ border: 1px solid #ccc; padding: 6px; text-align: center; }}
        th {{ background-color: #f2f2f2; }}
        h4 {{ border-bottom: 2px solid #333; padding-bottom: 5px; margin-top: 20px; }}
    </style>
    </head>
    <body>
        <h3>本日のMagic Formula分析結果</h3>
        <p>スコア算出: ROE順位 + PER順位 (低いほど優秀)</p>
        
        {html_jp}
        {html_us}
        
        <br>
        <p>※詳細は添付のCSVファイルをご確認ください。</p>
    </body>
    </html>
    """
    
    msg.attach(MIMEText(full_html, 'html'))

    # ファイル添付 (Japan)
    if path_jp and os.path.exists(path_jp):
        with open(path_jp, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(path_jp))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(path_jp)}"'
        msg.attach(part)

    # ファイル添付 (US)
    if path_us and os.path.exists(path_us):
        with open(path_us, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(path_us))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(path_us)}"'
        msg.attach(part)

    # 送信
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(GMAIL_USER, GMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        print("メール送信完了！")
    except Exception as e:
        print(f"送信失敗: {e}")

else:
    print("メール設定なしのためスキップ")