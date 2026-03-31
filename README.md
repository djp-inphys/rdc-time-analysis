# RDC Time Analysis

A scientific data analysis project investigating the **timing reliability and BLE connectivity** of CB100 radiation dosimeter devices. The project delivers two primary findings:

1. **Thermal Clock Drift** — The CB100's internal RC oscillator drifts with temperature (~+1 ms per +5°C), causing systematic lag accumulation when relying on device timestamps.
2. **BLE Dropout Root Cause** — Three specific devices intermittently disconnect due to a blocking ADC read in firmware that freezes the BLE stack long enough to trigger connection timeouts on stricter host OS configurations (e.g., Ubuntu/Linux).

---

## Table of Contents

- [Background](#background)
- [Project Structure](#project-structure)
- [Key Findings](#key-findings)
- [Scripts](#scripts)
- [Data Format](#data-format)
- [Exported Outputs](#exported-outputs)
- [Setup & Requirements](#setup--requirements)
- [Usage](#usage)
- [Documentation](#documentation)
- [Batch Data Summary](#batch-data-summary)

---

## Background

The **CB100** is a Bluetooth Low Energy (BLE) radiation data logger built on the **STM32WB5MMG** microcontroller. Devices transmit a 20-byte `Telemetry_SensorData` packet (timestamp, pulse count, charge count, ADC, min/max/mean/std) at approximately 200 ms intervals to a host PC running `old/CB100_BLE.py`.

This project analyses the resulting log files to characterise two independent failure modes:

| Failure Mode | Affected Devices | Root Cause |
|---|---|---|
| Thermal clock drift | All devices | LSI (internal RC) oscillator used instead of LSE (crystal) for BLE tick timing |
| Intermittent BLE dropout | CB100-2600577, CB100-2598385, CB100-2599429 | Blocking ADC read in firmware stalls BLE stack |

---

## Project Structure

```
rdc-time-analysis/
│
├── cb100_eda_lib.py               # Shared library: parsing, processing, exporting
├── thermal_drift.py               # Thermal clock drift analysis script
├── dropout_gaps.py                # BLE dropout / gap detection script
├── health-check.py                # 3D device health clustering visualisation
├── eda-2.0.py                     # Monolithic EDA + collision analysis pipeline
├── preparse_txt_logs.py           # Standalone log pre-parser and cache builder
│
├── rdc-captures/                  # Raw .txt log files (all formats)
│   ├── Batch*.txt                 #   Short-duration batch captures (CB100_BLE.py)
│   ├── LongTimeOverNight_*.txt    #   Multi-hour overnight captures (CSV timestamp format)
│   └── WeekendCapture_*.txt       #   Extended weekend stress-test captures
│
├── parsed_cache/                  # Auto-created binary cache (gitignored)
│   └── parsed_readings.pkl.gz     #   Gzip-compressed DataFrame from preparse_txt_logs.py
│
├── session_exports/               # Parsed per-device session CSVs
├── gap_exports/                   # Dropout gap CSVs (per-device + combined)
├── thermal_exports/               # Thermal drift summary and significance CSVs
├── collision_exports/             # Tick collision timing CSVs
├── real-gaps/                     # Curated ground-truth gap CSVs (4 devices)
│
├── Thermal drift.md               # Thermal analysis technical report
├── Technical Investigation into Thermal Tinming Drift.md
├── Analysis of signal separation.md
│
├── device_health_3d.html          # Interactive 3D health clustering chart
│
├── old/                           # Archived / predecessor scripts
│   ├── CB100_BLE.py               #   BLE data collector application (upstream)
│   ├── eda-v1.0.py
│   ├── eda-1.5.py
│   ├── eda.py
│   ├── eda-2.0.py
│   └── main.py
│
└── .vscode/
    ├── settings.json              # autopep8 formatter, Miniconda terminal
    └── launch.json                # debugpy launch config
```

---

## Key Findings

### 1. Thermal Clock Drift

| Temperature Range | Median Δt | Change | Significance |
|---|---|---|---|
| 20°C – 25°C | ~203 ms | — | — |
| 25°C – 30°C | ~204 ms | +1 ms | **Significant** |
| 35°C – 40°C | ~207–208 ms | +4–5 ms cumulative | **Significant** |

- **Rate:** ~+1 ms per +5°C rise (~0.5% frequency slowdown over 20°C).
- **Statistical method:** Permutation tests (2,000 permutations) on median Δt between adjacent 5°C bins, corrected with Benjamini-Hochberg FDR (α = 0.05).
- **Root cause:** STM32WB5MMG firmware uses the LSI internal RC oscillator (±1% tolerance, temperature-dependent) rather than the LSE 32.768 kHz crystal.
- **Recommendation:** Do not rely on CB100 device timestamps for precise timing; use host PC log-reception timestamps instead. A firmware fix switching to LSE would resolve the drift.

### 2. BLE Dropouts

- Three devices (`CB100-2600577`, `CB100-2598385`, `CB100-2599429`) exhibit intermittent disconnections with 400 ms – 10 s gaps.
- Signal-collision analysis ruled out BLE RF interference as the cause.
- Root cause identified as a **blocking ADC read** in firmware that stalls the BLE stack beyond the host's connection supervision timeout — particularly pronounced on Linux/Ubuntu hosts with stricter BLE timeout enforcement.

### 3. Device Health Classification

Using three features derived from tick-collision data, devices are clustered into three health classes:

| Class | Devices | Description |
|---|---|---|
| **Failing** | CB100-2600577, CB100-2598385, CB100-2599429 | High timeout ratio, right-tail skewness, poor IQR stability |
| **At-Risk** | CB100-2595836 | Borderline performance on one or more metrics |
| **Healthy** | All remaining devices | Stable timing, low dropout rate |

Each device is scored on three features computed from its inter-arrival times (the millisecond gap between consecutive received packets):

#### Feature A — Timeout Ratio
The **fraction of inter-arrival intervals that exceed 250 ms**. Because healthy devices report at ~200 ms, any interval longer than 250 ms represents a packet that arrived noticeably late — a sign the BLE stack was stalled. Failing devices have a markedly higher proportion of these delayed packets than healthy ones.

> `timeout_ratio = (intervals > 250ms).mean()`

#### Feature B — Skewness
The **statistical skewness of the inter-arrival time distribution**. A healthy device produces a tight, roughly symmetric distribution centred near 200 ms. A problematic device accumulates a long right-hand tail (occasional very large gaps), which drives positive skewness. High positive skew indicates that while most packets arrive on time, a non-trivial number are severely delayed.

> `skew = intervals.skew()`

#### Feature C — Stability Index (IQR)
The **interquartile range (IQR = Q75 − Q25)** of inter-arrival times, measuring the tightness of the device's "heartbeat". A small IQR means the device reports at a very consistent cadence. A large IQR means the gap between packets varies widely, which correlates with unreliable BLE connectivity even when timeouts are not yet occurring.

> `iqr = intervals.quantile(0.75) − intervals.quantile(0.25)`

Together, a device with a **high Timeout Ratio**, **high Skewness**, and **high IQR** sits in a distinct region of the 3D feature space, separating Failing and At-Risk devices from Healthy ones at a glance in `device_health_3d.html`.

---

## Scripts

### `cb100_eda_lib.py` — Shared Library

The backbone used by all analysis scripts. Key classes:

| Class | Responsibility |
|---|---|
| `LogParser` | Recursively discovers `Batch*.txt` files; parses readings, temperature blocks, and dropout events via regex |
| `DataProcessor` | Builds DataFrames, interpolates temperature onto readings, computes gateway lag, filters by date, detects intra-file gaps |
| `ResultsExporter` | Writes CSV outputs with file-lock fallback |
| `AppConfig` | Frozen dataclass holding all runtime configuration |

### `thermal_drift.py` — Thermal Analysis

Primary script for clock drift investigation. Outputs:

- Lag vs. temperature scatter plots
- Per-device Δt boxplots by temperature bin
- Charge accumulation vs. time with event markers
- Permutation test significance tables

### `dropout_gaps.py` — Gap Detection

Detects intra-file dropout events above a configurable threshold. Supports both auto-detection and loading of curated `real-gaps/` CSVs for ground-truth comparison.

### `health-check.py` — 3D Device Health (collision-based)

Loads `collision_exports/all_collision_ticks_W49ms.csv`, computes per-device health features (Timeout Ratio, Right-Tail Skewness, IQR Stability Index), and renders an interactive Plotly 3D scatter chart. Output saved as `device_health_3d.html`.

### `plt_test.py` — Per-device Timing Distribution Analysis

Loads all `session_exports/all_device_*.csv` files and computes health-discriminating features directly from raw inter-sample times on **both** the device clock and the host clock.  Does not depend on the collision pipeline.

The script addresses a key limitation of `health-check.py`: the old fixed 250 ms timeout threshold misclassifies newer devices whose nominal transmission period is ~258 ms.  Here an **adaptive threshold** (1.25 × per-device median) is used, so devices with different nominal periods are judged on the same relative scale.

**Clock sync:** Before comparing inter-sample intervals the per-session median offset between the device clock and host clock is subtracted, removing timezone and epoch biases and leaving only genuine jitter.

**Outputs:**

| File | Contents |
|---|---|
| `device_timing_histograms.png` | Grid: device-clock Δt, host-clock Δt, residual lag jitter — one column per device |
| `device_timing_features.csv` | Per-device distribution statistics (median, IQR, skew, adaptive timeout ratio, lag jitter std) |
| `device_health_features.png` | 2-D scatter: IQR vs Skew, Timeout Ratio vs IQR, Timeout Ratio vs Lag Jitter — all coloured by health status |

**CLI arguments:**

| Argument | Default | Description |
|---|---|---|
| `--session-dir` | `session_exports` | Directory containing `all_device_*.csv` files |
| `--output-dir` | `.` | Where to write PNG and CSV outputs |
| `--max-gap-ms` | `1000` | Discard inter-sample gaps larger than this (ms); excludes session-boundary jumps from Δt histograms |
| `--session-split-s` | `5` | Device-clock gap (seconds) that marks a new monitoring-session boundary within a source file; each segment gets its own independent clock-sync offset so overnight multi-session files no longer pollute residual lag with day-scale drift |
| `--timeout-factor` | `1.25` | Adaptive timeout threshold as a multiple of each device's own median inter-sample time |
| `--no-plots` | `False` | Skip all matplotlib output; write features CSV only |
| `--log-level` | `INFO` | Logging verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `--log-file` | — | Optional path to mirror log output to a file |

### `eda-2.0.py` — Monolithic EDA & Collision Analysis Pipeline

Full analysis pipeline in a single script. Parses raw `.txt` logs (or loads a pre-built binary cache via `--parsed-binary-path`), computes per-device session statistics, detects dropout gaps, and optionally runs the tick-collision analysis that produces `collision_exports/all_collision_ticks_W49ms.csv`.

Key CLI arguments:

| Argument | Default | Description |
|---|---|---|
| `--parsed-binary-path` | `parsed_cache/parsed_by_device.pkl.gz` | Load from binary cache instead of re-parsing `.txt` files |
| `--rebuild-parsed-binary` | `False` | Force re-parse even when cache exists |
| `--build-parsed-binary-only` | `False` | Write cache then exit without running analysis |
| `--collision-analysis` | `False` | Enable tick-collision export |
| `--collision-intra-gap-ms` | `1000` | Max intra-session gap (ms); larger gaps split active segments |
| `--real-gaps-dir` | — | Directory of curated ground-truth gap CSVs |

### `preparse_txt_logs.py` — Standalone Log Pre-Parser

Parses all CB100 `.txt` log files **once**, writes a gzip-compressed binary cache, and displays interactive per-file diagnostic plots.  Run this first before `eda-2.0.py` to dramatically reduce repeated parse times on large overnight captures.

Key CLI arguments:

| Argument | Default | Description |
|---|---|---|
| `--root` | `.` | Root directory to search |
| `--pattern` | `rdc-captures/*.txt` | Glob pattern for log file discovery |
| `--output-binary` | `parsed_cache/parsed_readings.pkl.gz` | Path for the binary cache output |
| `--plot-files` | `*` | Comma-separated glob patterns selecting files to plot; `''` skips all plots |
| `--max-gap-ms` | `10000` | Histogram x-axis clip (ms) — does not affect stored data |
| `--log-level` | `INFO` | Logging verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `--log-file` | — | Optional path to mirror log output to a file |

The generated cache is a `pandas.DataFrame` persisted with `df.to_pickle(..., compression='gzip')` and can be reloaded directly:

```python
import pandas as pd
df = pd.read_pickle("parsed_cache/parsed_readings.pkl.gz", compression="gzip")
```

---

## Data Format

### Input: Batch Log Files (`rdc-captures/Batch*.txt`)

Plain-text files produced by `CB100_BLE.py`.  Lines use a `[HH:MM:SS.mmm]` bracket prefix:

```
# Sensor reading
[13:23:16.919] CB100-2600577--> TS: 1766751796.303 | Pulse: 0 | Charge: 75 | ADC: 0 | ...

# Dropout event
[13:23:16.906] DATA_LOSS_DETECTED: CB100-2599429 - 13606s jump detected
```

### Input: Overnight / Weekend Log Files (`rdc-captures/LongTimeOverNight_*.txt`, `WeekendCapture_*.txt`)

Produced by a newer logger variant.  Lines use a CSV prefix (`HH:MM:SS.mmm,`) rather than brackets:

```
Timestamp,Message
17:01:11.575,CB100-2597625--> TS: 1774368070.079 | Pulse: 0 | Charge: 6 | ADC: 0 | ...
```

Both formats are handled transparently by `preparse_txt_logs.py` and `eda-2.0.py`.

### Binary Cache (`parsed_cache/parsed_readings.pkl.gz`)

Compressed `pandas.DataFrame` written by `preparse_txt_logs.py`:

| Column | Type | Description |
|---|---|---|
| `source_file` | str | Basename of originating `.txt` log |
| `device_uid` | str | Device identifier (e.g. `CB100-2597625`) |
| `captured_at` | datetime | Device-side timestamp (from TS field) |
| `log_time` | datetime / NaT | Host-PC reception timestamp (NaT if not parseable) |
| `pulse_count` | int | Pulse counter value |
| `charge_count` | int | Integrated charge counter |
| `adc_value` | int | Raw ADC reading (0 when not reported) |
| `inter_sample_ms` | float | Gap (ms) to previous reading for same device in same file; NaN for first reading |

### Input: Dose Calibration (`B00X/dose_XXXXXXX-v1.0.csv`)

Two-column files mapping charge counts to dose values. Twenty files across five batches.

### Parsed Session CSVs (`session_exports/`)

| Column | Description |
|---|---|
| `device_uid` | Device identifier (e.g., `CB100-2600577`) |
| `captured_at` | Device-side timestamp (Unix seconds) |
| `log_time` | Host PC reception timestamp |
| `pulse_count` | Pulse counter value |
| `charge_count` | Integrated charge counter |
| `adc_value` | Raw ADC reading |
| `temperature_c` | Ambient temperature (interpolated) |
| `lag_seconds_raw` | Raw host–device timestamp delta |
| `lag_seconds` | Offset-corrected lag |

---

## Exported Outputs

| Directory | Contents |
|---|---|
| `session_exports/` | Parsed readings per device (23 CSVs) |
| `gap_exports/` | Dropout gaps per device + combined (9 CSVs) |
| `thermal_exports/` | Drift summary, per-bin Δt stats, significance tables, drift rate by temp bin |
| `collision_exports/` | Tick collision timing data |
| `real-gaps/` | Curated ground-truth gap CSVs for 4 problem devices |
| `device_timing_histograms.png` | Per-device histogram grid (from `plt_test.py`) |
| `device_timing_features.csv` | Per-device distribution features (from `plt_test.py`) |
| `device_health_features.png` | 2-D health feature scatter plots (from `plt_test.py`) |

Key thermal export files:

| File | Description |
|---|---|
| `all_thermal_drift_summary.csv` | Per-device drift slope (s/hour), median lag, median temperature |
| `all_dt_temp_bins_per_device.csv` | Per-device, per-bin Δt statistics (n, median, mean) |
| `all_dt_temp_bin_significance_all_devices.csv` | Adjacent-bin permutation test results with BH-FDR q-values |
| `all_drift_rate_temp_bins.csv` | Instantaneous drift rate per temperature bin |

---

## Setup & Requirements

The project runs under **Miniconda on Windows** (`D:\ProgramData\miniconda3`). There is no `requirements.txt`; install dependencies manually into your environment:

```bash
conda activate <your-env>
pip install pandas numpy matplotlib plotly scipy
```

| Package | Purpose |
|---|---|
| `pandas` | All data processing |
| `numpy` | Numerical computation, interpolation |
| `matplotlib` | Static plots (boxplots, scatter, timelines) |
| `plotly` | Interactive 3D health chart |
| `scipy` | Statistical tests (optional, soft import) |
| `bleak` | BLE data collection only (`old/CB100_BLE.py`) |

Python version: **3.x** (standard library `tkinter`, `re`, `logging`, `dataclasses` also used).

---

## Usage

All scripts are run from the project root directory.

### Thermal Drift Analysis

```bash
python thermal_drift.py
```

With optional arguments:

```bash
python thermal_drift.py \
  --root . \
  --pattern "**/*.txt" \
  --filter-date all \
  --export-dir thermal_exports \
  --plots-dir thermal_plots \
  --lag-offset-seconds -3600.0 \
  --temp-bin-size-c 5.0 \
  --temp-bin-test-alpha 0.05 \
  --temp-bin-test-min-samples 200 \
  --temp-bin-test-permutations 2000 \
  --dt-max-ms 1000.0
```

### Dropout Gap Analysis

```bash
# Auto-detect gaps from log files
python dropout_gaps.py

# Use curated ground-truth gaps instead
python dropout_gaps.py --use-real-gaps --real-gaps-dir real-gaps
```

With optional arguments:

```bash
python dropout_gaps.py \
  --root . \
  --pattern "**/*.txt" \
  --filter-date all \
  --gap-threshold-ms 400.0 \
  --max-gap-ms 10000.0 \
  --gap-csv-dir gap_exports
```

### Pre-parse Log Files (recommended first step)

Build the binary cache for all captures and display diagnostic plots for all files:

```bash
python preparse_txt_logs.py
```

Limit plots to overnight files only:

```bash
python preparse_txt_logs.py --plot-files "LongTimeOverNight*,WeekendCapture*"
```

Build cache without any plots (fastest):

```bash
python preparse_txt_logs.py --plot-files ""
```

Custom root / pattern / output:

```bash
python preparse_txt_logs.py \
  --root . \
  --pattern "rdc-captures/*.txt" \
  --output-binary parsed_cache/parsed_readings.pkl.gz \
  --max-gap-ms 5000 \
  --log-level DEBUG \
  --log-file preparse.log
```

### EDA & Collision Analysis (`eda-2.0.py`)

Basic run (re-parses txt files):

```bash
python eda-2.0.py
```

Use the binary cache to skip parsing:

```bash
python eda-2.0.py --parsed-binary-path parsed_cache/parsed_readings.pkl.gz
```

Rebuild cache and run collision analysis:

```bash
python eda-2.0.py \
  --rebuild-parsed-binary \
  --collision-analysis \
  --collision-intra-gap-ms 1000
```

### Device Health Visualisation (collision-based, 3D)

```bash
python health-check.py
```

Opens an interactive 3D Plotly chart in your browser and saves `device_health_3d.html`.

### Per-device Timing Distribution Analysis

Run the full analysis (histograms + feature scatter plots + CSV):

```bash
python plt_test.py
```

Features CSV only (no plots, fastest):

```bash
python plt_test.py --no-plots
```

Custom session directory and output location:

```bash
python plt_test.py \
  --session-dir session_exports \
  --output-dir analysis_results \
  --max-gap-ms 1000 \
  --session-split-s 5 \
  --timeout-factor 1.25 \
  --log-level INFO \
  --log-file plt_test.log
```

---

## Documentation

Detailed technical reports are included in the project root:

| File | Contents |
|---|---|
| `Thermal drift.md` | Methodology, observed trend, root cause (RC oscillator), firmware recommendations |
| `Technical Investigation into Thermal Tinming Drift.md` | STM32WB5MMG-specific analysis, LSI vs. LSE diagnosis, power impact, full statistical appendix |
| `Analysis of signal separation.md` | BLE signal collision investigation; conclusion that dropouts are caused by blocking ADC read, not RF interference |

---

## Batch Data Summary

| Batch | Date | Notes |
|---|---|---|
| B001 | Dec 26, 2025 | 10 min, 20 min, 30 min runs |
| B002 | Dec 29, 2025 | Device D560 suspected battery failure mid-run |
| B003 | Dec 29, 2025 | |
| B004 | Dec 29, 2025 | |
| B005 | Dec 29, 2025 | |
| B006 | 2016 | Wrong date 2026 very high dose rate testing (alternate CSV format) |
