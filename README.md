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
├── eda-2.0.py                     # Legacy monolithic EDA script (reference only)
│
├── B001/ – B006/                  # Raw batch log files (input data)
│   ├── Batch*.txt                 #   BLE log files produced by CB100_BLE.py
│   ├── dose_XXXXXXX-v1.0.csv      #   Per-device dose calibration tables
│   └── Note*.txt                  #   Field notes (where present)
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

### `health-check.py` — 3D Device Health

Loads `collision_exports/all_collision_ticks_W49ms.csv`, computes per-device health features (Timeout Ratio, Right-Tail Skewness, IQR Stability Index), and renders an interactive Plotly 3D scatter chart. Output saved as `device_health_3d.html`.

### `eda-2.0.py` — Legacy Monolithic Script

Predecessor to the split architecture above. Contains the same analysis logic in a single file. Kept for reference; not recommended for new work.

---

## Data Format

### Input: Batch Log Files (`B001/Batch*.txt`)

Plain-text files produced by `CB100_BLE.py`. Two line types:

```
# Sensor reading
[13:23:16.919] CB100-2600577--> TS: 1766751796.303 | Pulse: 0 | Charge: 75 | ADC: 0 | ...

# Dropout event
[13:23:16.906] DATA_LOSS_DETECTED: CB100-2599429 - 13606s jump detected
```

B006 additionally contains a legacy CSV variant (2016 high-dose-rate testing) with `Timestamp,Message` columns.

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
| `session_exports/` | Parsed readings per device (20 CSVs) |
| `gap_exports/` | Dropout gaps per device + combined (9 CSVs) |
| `thermal_exports/` | Drift summary, per-bin Δt stats, significance tables, drift rate by temp bin |
| `collision_exports/` | Tick collision timing data |
| `real-gaps/` | Curated ground-truth gap CSVs for 4 problem devices |

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

### Device Health Visualisation

```bash
python health-check.py
```

Opens an interactive 3D Plotly chart in your browser and saves `device_health_3d.html`.

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
