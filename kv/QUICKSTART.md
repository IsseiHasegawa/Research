# クイックスタートガイド

## 現在のディレクトリを確認

```bash
pwd
```

## 正しい実行手順

### 1. kvディレクトリに移動

**現在 `build` ディレクトリにいる場合:**
```bash
cd ..  # 親ディレクトリ（kv/）に戻る
```

**または、絶対パスで移動:**
```bash
cd /Users/issei/Research/kv
```

### 2. Python依存関係のインストール

```bash
pip3 install -r experiments/requirements.txt
```

**権限エラーが出る場合:**
```bash
pip3 install --user -r experiments/requirements.txt
```

### 3. 実験の実行

```bash
python3 experiments/run_experiments.py
```

### 4. 結果の分析

```bash
python3 analysis/analyze.py
python3 analysis/plot_heatmap.py
```

## ディレクトリ構造の確認

```bash
# kvディレクトリにいることを確認
pwd
# 出力: /Users/issei/Research/kv

# 必要なファイルが存在するか確認
ls experiments/requirements.txt
ls analysis/analyze.py
ls build/kvnode
```
