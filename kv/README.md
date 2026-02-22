# Distributed KV Store - 実行ガイド

## セットアップ

### 1. C++ビルド

```bash
cd kv
mkdir -p build
cd build
cmake ..
make
```

これで `build/kvnode` が生成されます。

### 2. Python依存関係のインストール

```bash
cd kv
pip3 install -r experiments/requirements.txt
```

必要なパッケージ:
- `requests` (HTTPクライアント)
- `pandas` (データ分析)
- `matplotlib` (可視化)

## 実行方法

### 実験の実行

```bash
cd kv
python3 experiments/run_experiments.py
```

このスクリプトは:
- 複数のハートビート間隔 (50, 100, 200ms) とタイムアウト (200, 400, 800, 1200ms) の組み合わせで実験を実行
- 各実験でリーダーをクラッシュさせ、障害検出時間とダウンタイムを測定
- 結果を `runs/` ディレクトリに保存

### 個別ノードの手動起動

#### リーダー (ポート8001)

```bash
./build/kvnode \
  --id A \
  --port 8001 \
  --leader 1 \
  --peers "B@127.0.0.1:8002,C@127.0.0.1:8003" \
  --hb_interval 100 \
  --hb_timeout 500 \
  --log runs/test/A.jsonl
```

#### フォロワー B (ポート8002)

```bash
./build/kvnode \
  --id B \
  --port 8002 \
  --leader 0 \
  --leader_addr 127.0.0.1:8001 \
  --hb_interval 100 \
  --hb_timeout 500 \
  --log runs/test/B.jsonl
```

#### フォロワー C (ポート8003)

```bash
./build/kvnode \
  --id C \
  --port 8003 \
  --leader 0 \
  --leader_addr 127.0.0.1:8001 \
  --hb_interval 100 \
  --hb_timeout 500 \
  --log runs/test/C.jsonl
```

### APIの使用例

#### PUT (リーダーに書き込み)

```bash
curl -X POST http://127.0.0.1:8001/put?rid=test-1 \
  -H "Content-Type: application/json" \
  -d '{"key": "x", "value": "hello"}'
```

#### GET (任意のノードから読み込み)

```bash
curl -X POST http://127.0.0.1:8002/get?rid=test-2 \
  -H "Content-Type: application/json" \
  -d '{"key": "x"}'
```

## 結果の分析

### 1. メトリクスの抽出

```bash
cd kv
python3 analysis/analyze.py
```

これで `analysis_out/metrics.csv` が生成されます。

### 2. ヒートマップの生成

```bash
python3 analysis/plot_heatmap.py
```

生成されるファイル:
- `plots/heatmap_downtime.png` - ダウンタイムのヒートマップ
- `plots/heatmap_detection.png` - 障害検出時間のヒートマップ

### 3. タイムラインの可視化

**利用可能なrun_idを確認:**
```bash
ls runs/
```

**タイムラインを生成:**
```bash
python3 analysis/plot_timeline.py <run_id>
```

**実行例:**
```bash
# 例: 利用可能なrun_idの1つを使用
python3 analysis/plot_timeline.py leader_crash_hb100_to400_1771787322474
```

**注意:** `<run_id>` は実際のrun_idに置き換えてください。`<run_id>` という文字列のまま実行するとエラーになります。

生成されるファイル:
- `plots/timeline_<run_id>.png` - PUT成功/失敗のタイムライン

## ディレクトリ構造

```
kv/
├── build/              # ビルド成果物
│   └── kvnode          # 実行可能ファイル
├── src/                # C++ソースコード
├── vendor/             # サードパーティライブラリ (httplib.h)
├── experiments/        # 実験スクリプト
│   └── run_experiments.py
├── analysis/           # 分析スクリプト
│   ├── analyze.py
│   ├── plot_heatmap.py
│   └── plot_timeline.py
├── runs/               # 実験結果 (自動生成)
│   └── <run_id>/
│       ├── meta.json
│       ├── fault.json
│       ├── client_events.jsonl
│       ├── A.jsonl, B.jsonl, C.jsonl
│       └── ...
├── analysis_out/       # 分析結果 (自動生成)
│   └── metrics.csv
└── plots/              # グラフ (自動生成)
    └── ...
```

## トラブルシューティング

### ビルドエラー

- **nlohmann/jsonが見つからない**: CMakeのFetchContentが自動的にダウンロードします。ネットワーク接続を確認してください。
- **httplib.hが見つからない**: `vendor/httplib.h` が存在することを確認してください。

### 実行時エラー

- **ポートが使用中**: 他のプロセスがポート8001-8003を使用していないか確認してください。
- **pandas/matplotlibが見つからない**: `pip3 install -r experiments/requirements.txt` を実行してください。

### 実験が完了しない

- 各実験は約6秒かかります。複数の組み合わせを実行する場合、時間がかかります。
- プロセスが残っている場合は `pkill kvnode` で終了できます。
