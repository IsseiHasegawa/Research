# Quick Start Guide

## Check Current Directory

```bash
pwd
```

## Correct Run Steps

### 1. Move to kv Directory

**If you are in the `build` directory:**
```bash
cd ..  # Go back to parent directory (kv/)
```

**Or use absolute path:**
```bash
cd /Users/issei/Research/kv
```

### 2. Install Python Dependencies

```bash
pip3 install -r experiments/requirements.txt
```

**If you get a permission error:**
```bash
pip3 install --user -r experiments/requirements.txt
```

### 3. Run Experiments

```bash
python3 experiments/run_experiments.py
```

### 4. Analyze Results

```bash
python3 analysis/analyze.py
python3 analysis/plot_heatmap.py
```

## Verify Directory Structure

```bash
# Confirm you are in the kv directory
pwd
# Output: /Users/issei/Research/kv

# Verify required files exist
ls experiments/requirements.txt
ls analysis/analyze.py
ls build/kvnode
```
