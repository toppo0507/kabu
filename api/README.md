# api

apiを用いて株価の分析をする奴。基本的にはYahoo!Japan Finaceが提供するAPIを利用

## TODO

- パッケージのインストールなどの環境整備の手順書
- 特定の株価と為替のデータをダウンロード
- 取り出された株価同士の相関係数や因果関係の解析
  - 相関係数の計算
  - 時系列としてのラグの推論

## 使い方

### uv のインストール

- https://note.com/npaka/n/n44c54312fb04

### ローカル用の環境変数をコピーする

```bash
$ cp .env .env.local
```

以降は.env.localをいい感じにする。

### データを取り出す

現在の設定は、日経平均に

```bash
$ uv run --env-file .env.local main.py --mode download
```

### ダウンロードしたデータを分析

```bash
$ uv run --env-file .env.local main.py --mode annalise
```

### グラフの可視化

```bash
$ uv run --env-file .env.local main.py --mode visualization
```

### 参考ページ

- https://qiita.com/tapitapi/items/9459362d8aee25137647
- https://financialmarkets.hatenablog.com/entry/2024/06/26/165744
- https://indexes.nikkei.co.jp/nkave/index/component?idx=nk225
- https://www.sbisec.co.jp/ETGate/WPLETmgR001Control?OutSide=on&getFlg=on&burl=search_market&cat1=market&cat2=info&dir=info&file=market_meigara_225.html

## 利用技術

- wsl
- uv (0.9.13)
  - python3.13
  - yfinance
  - matplotlib
  - networkx
  - scipy
  - setuptools
  - beautiful soup4
