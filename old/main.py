"""
Charge vs Temperature correlation + per-device calibration for CB100 logs.

This script parses one or more CB100 log files and:
- Extracts **charge readings** from lines containing `Charge: <int>`
- Extracts **ambient temperature readings** from "Temperature Information from ..." blocks
- Correlates charge readings to temperature *intervals* (between consecutive temperature samples)
- Fits a **per-Serial polynomial model**: Avg_Charge = f(Temperature)
- Produces a temperature-normalized charge value referenced to `REF_TEMP_C`

Typical log formats parsed
--------------------------
Charge lines (single line, repeated often):
    [13:23:45.319] CB100-2595836--> ... | Charge:     74 | ...

Temperature blocks (header line + value within the next few lines):
    [13:23:45.296] Temperature Information from CB100-2598385:
    Ambient Temperature: 37.25°C

Key assumptions / behaviors
---------------------------
- Log timestamps are parsed from the bracketed time `[HH:MM:SS.mmm]`. Since the logs do
  not include a date, `BASE_DATE` supplies the calendar date.
- Temperature parsing looks ahead up to 4 subsequent lines after the header to find
  "Ambient Temperature:".
- Malformed/unexpected lines are skipped (best-effort parsing).

Outputs
-------
- `charge_vs_temp_calibrated_<REF_TEMP_C>C_deg<FIT_DEGREE>.csv`:
    Correlated points + fitted values + temperature-normalized charge per Serial.
- `charge_vs_temp_models_deg<FIT_DEGREE>.csv`:
    Per-Serial polynomial coefficients and fit quality (R²).
- `charge_vs_temp_with_fit_per_serial_deg<FIT_DEGREE>.png`:
    Plot of Avg_Charge vs Temperature with fitted curves.
"""

import pandas as pd
import matplotlib.pyplot as plt
import re
from datetime import datetime
import numpy as np
import argparse
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from scipy.interpolate import splrep, splev

# --- Configuration ---
# Logs contain only time-of-day; a date is injected during parsing.
# If not provided via CLI, we attempt to infer it from filenames like:
#   Batch001_2025-12-26_10min.txt
DEFAULT_BASE_DATE = datetime(2025, 12, 26)
# Reference temperature for charge normalization/calibration.
REF_TEMP_C = 21.0  # Reference temperature for charge normalization/calibration
# Per-device polynomial degree. 1 = linear, 2 = quadratic, 3 = cubic, etc.
FIT_DEGREE = 2
# Fit type: "poly" uses np.polyfit, "smooth" uses a smoothing spline (SciPy).
FIT_TYPE = "poly"


def _ref_label(temp_c: float) -> str:
    # Used for column names; keep historical behavior for integer temps.
    if abs(temp_c - round(temp_c)) < 1e-9:
        return f"{int(round(temp_c))}C"
    return f"{temp_c:g}C"


def _safe_filename_fragment(s: str) -> str:
    # Make serial safe for filenames on Windows.
    return re.sub(r'[^A-Za-z0-9_.-]+', '_', str(s)).strip('_')


def parse_log_time(time_str: str, base_date: datetime) -> Optional[datetime]:
    """
    Convert a log time-of-day string into an absolute datetime using `base_date`.

    Args:
        time_str: Time string from the log in the form "HH:MM:SS.mmm" (or with more
            fractional-second digits if present).

    Returns:
        A `datetime` on `base_date` with the parsed time-of-day, or `None` if parsing fails.
    """
    try:
        t = datetime.strptime(time_str, "%H:%M:%S.%f")
        return base_date.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=t.microsecond)
    except ValueError:
        return None


def parse_files(file_list: Iterable[Path], base_date: datetime) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parse one or more CB100 log files into charge and temperature DataFrames.

    Parsed records (best effort):
    - Charge record keys: `Serial`, `Timestamp`, `Charge`
    - Temperature record keys: `Serial`, `Timestamp`, `Temperature` (ambient, °C)

    A charge line is detected by checking for `"-->"` and `"Charge:"` and then extracting:
    - Timestamp from `[HH:MM:SS.mmm]`
    - Serial from `CB100-<digits>`
    - Charge from `Charge: <int>`

    A temperature block is detected by matching `"Temperature Information from"` and then
    looking ahead up to 4 lines for `"Ambient Temperature:"`.

    Args:
        file_list: Iterable of log file paths.
        base_date: Calendar date to inject when parsing `[HH:MM:SS.mmm]` timestamps.

    Returns:
        (df_charge, df_temp) as pandas DataFrames.
    """
    charge_data = []
    temp_data = []

    for fname in file_list:
        with open(fname, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()

        i = 0
        while i < len(lines):
            line = lines[i].strip()

            # 1. Parse Charge Data
            if "-->" in line and "Charge:" in line:
                try:
                    time_match = re.search(
                        r'\[(\d{2}:\d{2}:\d{2}\.\d{3})\]', line)
                    serial_match = re.search(r'(CB100-\d+)', line)
                    charge_match = re.search(r'Charge:\s*(\d+)', line)

                    if time_match and serial_match and charge_match:
                        charge_data.append({
                            'Serial': serial_match.group(1),
                            'Timestamp': parse_log_time(time_match.group(1), base_date),
                            'Charge': int(charge_match.group(1))
                        })
                except Exception:
                    pass

            # 2. Parse Temperature Headers
            elif "Temperature Information from" in line:
                try:
                    time_match = re.search(
                        r'\[(\d{2}:\d{2}:\d{2}\.\d{3})\]', line)
                    serial_match = re.search(r'(CB100-\d+)', line)

                    if time_match and serial_match:
                        log_time = parse_log_time(time_match.group(1), base_date)
                        serial = serial_match.group(1)

                        # Look ahead for Ambient Temperature
                        temp_val = None
                        for offset in range(1, 5):
                            if i + offset < len(lines):
                                next_line = lines[i + offset].strip()
                                if "Ambient Temperature:" in next_line:
                                    temp_match = re.search(
                                        r'Ambient Temperature:\s*([\d\.]+)', next_line)
                                    if temp_match:
                                        temp_val = float(temp_match.group(1))
                                        break

                        if temp_val is not None:
                            temp_data.append({
                                'Serial': serial,
                                'Timestamp': log_time,
                                'Temperature': temp_val
                            })
                except Exception:
                    pass

            i += 1

    return pd.DataFrame(charge_data), pd.DataFrame(temp_data)


def clean_temperature_data(df_temp):
    """
    Clean/normalize temperature samples by aligning to 1-second boundaries.

    The correlation step uses temperature readings as interval boundaries. Temperature logs
    can have minor timestamp jitter, so we round temperature timestamps to the nearest
    second and group any duplicates by mean temperature.

    Args:
        df_temp: Temperature DataFrame from `parse_files`, with columns:
            `Serial`, `Timestamp`, `Temperature`.

    Returns:
        A DataFrame with columns: `Serial`, `Timestamp_Rounded`, `Temperature`.
    """
    if df_temp.empty:
        return df_temp.copy()

    # Ensure Timestamp is datetime64 for .dt access.
    df_temp = df_temp.copy()
    df_temp['Timestamp'] = pd.to_datetime(df_temp['Timestamp'], errors='coerce')
    df_temp = df_temp.dropna(subset=['Timestamp'])

    # Round to nearest 1 second
    df_temp['Timestamp_Rounded'] = df_temp['Timestamp'].dt.round('1s')

    # Group by Serial and Rounded Timestamp, taking the mean of Temperature
    # (This handles cases where logs might have slight jitter)
    df_clean = df_temp.groupby(['Serial', 'Timestamp_Rounded'])[
        'Temperature'].mean().reset_index()

    return df_clean


def correlate_data(df_charge, df_temp_clean):
    """
    Correlate charge readings to temperature intervals and compute per-interval averages.

    For each Serial:
    - Consider consecutive temperature samples as interval boundaries
    - Average all charge readings where (t_start, t_end] to produce a single point
    - Use average temperature of the boundary temperatures as the point's Temperature

    Args:
        df_charge: DataFrame with columns `Serial`, `Timestamp`, `Charge`.
        df_temp_clean: DataFrame with columns `Serial`, `Timestamp_Rounded`, `Temperature`.

    Returns:
        DataFrame with columns `Serial`, `Temperature`, `Avg_Charge`.
    """
    results = []

    # Ensure sorted
    if df_charge.empty or df_temp_clean.empty:
        return pd.DataFrame(results)

    df_charge = df_charge.copy()
    df_charge['Timestamp'] = pd.to_datetime(df_charge['Timestamp'], errors='coerce')
    df_charge = df_charge.dropna(subset=['Timestamp'])

    df_charge = df_charge.sort_values(['Serial', 'Timestamp'])
    df_temp_clean = df_temp_clean.sort_values(['Serial', 'Timestamp_Rounded'])

    unique_serials = df_temp_clean['Serial'].unique()

    for serial in unique_serials:
        # Get temp points for this serial
        temps = df_temp_clean[df_temp_clean['Serial']
                              == serial].reset_index(drop=True)
        charges = df_charge[df_charge['Serial'] == serial]

        for i in range(len(temps) - 1):
            # Use the ROUNDED timestamps for the interval boundaries
            t_start = temps.loc[i, 'Timestamp_Rounded']
            t_end = temps.loc[i+1, 'Timestamp_Rounded']

            temp_start = temps.loc[i, 'Temperature']
            temp_end = temps.loc[i+1, 'Temperature']

            avg_temp = (temp_start + temp_end) / 2.0

            # Filter charge data
            mask = (charges['Timestamp'] > t_start) & (
                charges['Timestamp'] <= t_end)
            interval_charges = charges.loc[mask, 'Charge']

            if len(interval_charges) > 0:
                avg_charge = interval_charges.mean()
                results.append({
                    'Serial': serial,
                    'Temperature': avg_temp,
                    'Avg_Charge': avg_charge
                })

    return pd.DataFrame(results)


def fit_charge_vs_temperature(df_results, degree=1):
    """
    Fits Avg_Charge as a function of Temperature using a polynomial fit.

    Returns:
        (coeffs, r2)
        - coeffs: numpy array of polynomial coefficients (highest power first)
        - r2: coefficient of determination
    """
    df_fit = df_results[['Temperature', 'Avg_Charge']].dropna()
    if len(df_fit) < degree + 1:
        return None, None

    x = df_fit['Temperature'].to_numpy(dtype=float)
    y = df_fit['Avg_Charge'].to_numpy(dtype=float)

    coeffs = np.polyfit(x, y, deg=degree)
    p = np.poly1d(coeffs)

    y_hat = p(x)
    ss_res = np.sum((y - y_hat) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot != 0 else 1.0

    return coeffs, r2


def _prepare_xy_for_fit(df_results):
    df_fit = df_results[['Temperature', 'Avg_Charge']].dropna()
    if df_fit.empty:
        return None, None

    # Sort by Temperature and collapse duplicate temperatures by mean charge to keep the
    # spline fitter well-behaved.
    df_fit = df_fit.groupby('Temperature', as_index=False)['Avg_Charge'].mean()
    df_fit = df_fit.sort_values('Temperature')

    x = df_fit['Temperature'].to_numpy(dtype=float)
    y = df_fit['Avg_Charge'].to_numpy(dtype=float)
    return x, y


def fit_charge_vs_temperature_smooth(df_results, k=3, s=None, s_factor=1.0):
    """
    Fits Avg_Charge as a function of Temperature using a smoothing spline.

    Uses SciPy's FITPACK wrapper (`splrep`/`splev`) and returns a serializable tck tuple.

    Args:
        df_results: DataFrame containing Temperature and Avg_Charge columns.
        k: Spline degree (1..5). Higher values can follow curvature more easily, but may also
            overfit and can behave poorly near the ends with sparse/noisy data. Typically 3
            (cubic) is a good default.
        s: Smoothing factor controlling the trade-off between closeness-to-data and smoothness.
            - s = 0 produces an *interpolating* spline (passes through points) and will
              generally overfit noisy data.
            - Increasing s produces a smoother curve that does not try to hit every point.
            - If None, this code chooses an automatic s based on the scale/variance of y and
              the number of points, then applies `s_factor` as a multiplier.
        s_factor: Multiplier applied only when `s` is not provided. Larger values make the
            automatically-chosen spline smoother; smaller values make it track the data more
            closely.

    Returns:
        (tck, r2, s_used)
        - tck: tuple (t, c, k) from splrep, suitable for splev
        - r2: coefficient of determination on the input points
        - s_used: smoothing factor used
    """
    x, y = _prepare_xy_for_fit(df_results)
    if x is None:
        return None, None, None

    # Need at least k+1 points for a spline of degree k.
    if len(x) < (k + 1):
        return None, None, None

    if s is None:
        # Heuristic: scale s by data variance and sample count.
        # Larger s => smoother curve. s≈0 => interpolating spline.
        y_var = float(np.var(y)) if len(y) > 1 else 0.0
        base = float(len(x)) * (y_var if y_var > 0 else 1.0)
        s_used = float(s_factor) * base
    else:
        s_used = float(s)

    tck = splrep(x, y, k=int(k), s=s_used)
    y_hat = np.asarray(splev(x, tck), dtype=float)

    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - float(np.mean(y))) ** 2))
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot != 0 else 1.0

    return tck, r2, s_used


def fit_and_calibrate_per_serial(
    df_results,
    fit_type,
    degree,
    ref_temp_c,
    ref_temps_c: Optional[Sequence[float]] = None,
    smooth_k=3,
    smooth_s=None,
    smooth_s_factor=1.0,
):
    """
    Fit Avg_Charge vs Temperature *per Serial* and calibrate each point to ref_temp_c.

    Adds to df_results:
      - Avg_Charge_Fit: fitted Avg_Charge at that Temperature for that Serial
      - Avg_Charge_@<ref>: temperature-corrected charge referenced to one or more reference temps

    Calibration formula (per Serial):
        Let p(T) be the fitted polynomial.
        - fitted_at_point = p(T_point)
        - fitted_at_ref   = p(T_ref)
        Then:
            charge_at_ref = Avg_Charge - (fitted_at_point - fitted_at_ref)

    Also returns a per-serial model table with coefficients and R^2.
    """
    if df_results.empty:
        return df_results.copy(), pd.DataFrame()

    df_out = df_results.copy()
    df_out['Avg_Charge_Fit'] = np.nan

    # Compute calibration columns for each reference temperature requested.
    if ref_temps_c is None:
        ref_temps: List[float] = [float(ref_temp_c)]
    else:
        ref_temps = [float(x) for x in ref_temps_c]
    # Ensure primary ref is included.
    ref_temps.append(float(ref_temp_c))
    # De-duplicate while preserving order.
    seen = set()
    ref_temps_unique: List[float] = []
    for t in ref_temps:
        k = round(float(t), 6)
        if k not in seen:
            seen.add(k)
            ref_temps_unique.append(float(t))

    for t in ref_temps_unique:
        df_out[f'Avg_Charge_@{_ref_label(t)}'] = np.nan

    model_rows = []

    for serial, sub in df_results.groupby('Serial', sort=True):
        fit_type_norm = (fit_type or "poly").strip().lower()

        if fit_type_norm == "smooth":
            tck, r2, s_used = fit_charge_vs_temperature_smooth(
                sub, k=smooth_k, s=smooth_s, s_factor=smooth_s_factor
            )
            if tck is None:
                row = {
                    'Serial': serial,
                    'FitType': 'smooth',
                    'Degree': np.nan,
                    'SplineK': smooth_k,
                    'SplineS': np.nan,
                    'R2': np.nan,
                    'Ref_Temp_C': float(ref_temp_c),
                    'Ref_Fit': np.nan,
                    'Ref_Fit_19C': np.nan,
                    'Ref_Fit_21C': np.nan,
                    'Coeffs_HighToLow': None,
                    'SplineT': None,
                    'SplineC': None,
                    'Note': 'Not enough points to fit'
                }
                model_rows.append(row)
                continue

            idx = sub.index
            fitted = np.asarray(splev(sub['Temperature'].astype(float).to_numpy(), tck), dtype=float)
            df_out.loc[idx, 'Avg_Charge_Fit'] = fitted

            # Apply calibration(s)
            for t_ref in ref_temps_unique:
                ref_fit = float(splev(t_ref, tck))
                df_out.loc[idx, f'Avg_Charge_@{_ref_label(t_ref)}'] = sub['Avg_Charge'] - (fitted - ref_fit)

            t, c, k = tck
            row = {
                'Serial': serial,
                'FitType': 'smooth',
                'Degree': np.nan,
                'SplineK': int(k),
                'SplineS': float(s_used),
                'R2': float(r2),
                'Ref_Temp_C': float(ref_temp_c),
                'Ref_Fit': float(splev(float(ref_temp_c), tck)),
                'Ref_Fit_19C': float(splev(19.0, tck)),
                'Ref_Fit_21C': float(splev(21.0, tck)),
                'Coeffs_HighToLow': None,
                'SplineT': ','.join(f'{v:.12g}' for v in np.asarray(t, dtype=float)),
                'SplineC': ','.join(f'{v:.12g}' for v in np.asarray(c, dtype=float)),
                'Note': ''
            }
            model_rows.append(row)

        else:
            coeffs, r2 = fit_charge_vs_temperature(sub, degree=degree)
            if coeffs is None:
                row = {
                    'Serial': serial,
                    'FitType': 'poly',
                    'Degree': degree,
                    'SplineK': np.nan,
                    'SplineS': np.nan,
                    'R2': np.nan,
                    'Ref_Temp_C': float(ref_temp_c),
                    'Ref_Fit': np.nan,
                    'Ref_Fit_19C': np.nan,
                    'Ref_Fit_21C': np.nan,
                    'Coeffs_HighToLow': None,
                    'SplineT': None,
                    'SplineC': None,
                    'Note': 'Not enough points to fit'
                }
                model_rows.append(row)
                continue

            p = np.poly1d(coeffs)

            idx = sub.index
            fitted = p(sub['Temperature'].astype(float))
            df_out.loc[idx, 'Avg_Charge_Fit'] = fitted

            for t_ref in ref_temps_unique:
                ref_fit = float(p(t_ref))
                df_out.loc[idx, f'Avg_Charge_@{_ref_label(t_ref)}'] = sub['Avg_Charge'] - (fitted - ref_fit)

            row = {
                'Serial': serial,
                'FitType': 'poly',
                'Degree': int(degree),
                'SplineK': np.nan,
                'SplineS': np.nan,
                'R2': float(r2),
                'Ref_Temp_C': float(ref_temp_c),
                'Ref_Fit': float(p(float(ref_temp_c))),
                'Ref_Fit_19C': float(p(19.0)),
                'Ref_Fit_21C': float(p(21.0)),
                'Coeffs_HighToLow': ','.join(f'{c:.12g}' for c in coeffs),
                'SplineT': None,
                'SplineC': None,
                'Note': ''
            }
            model_rows.append(row)

    df_models = pd.DataFrame(model_rows).sort_values('Serial')
    return df_out, df_models


def _evaluate_model_row(row: pd.Series, x: np.ndarray) -> np.ndarray:
    fit_type_row = str(row.get('FitType', 'poly')).strip().lower()
    x = np.asarray(x, dtype=float)

    if fit_type_row == "smooth" and pd.notna(row.get('SplineT')) and pd.notna(row.get('SplineC')):
        t = np.array([float(v) for v in str(row['SplineT']).split(',')], dtype=float)
        c = np.array([float(v) for v in str(row['SplineC']).split(',')], dtype=float)
        k = int(row.get('SplineK', 3))
        tck = (t, c, k)
        return np.asarray(splev(x, tck), dtype=float)

    coeffs_txt = row.get('Coeffs_HighToLow')
    if pd.notna(coeffs_txt):
        coeffs = np.array([float(v) for v in str(coeffs_txt).split(',')], dtype=float)
        p = np.poly1d(coeffs)
        return np.asarray(p(x), dtype=float)

    return np.full_like(x, np.nan, dtype=float)


def _write_per_serial_luts(
    *,
    dir_path: Path,
    df_results: pd.DataFrame,
    df_models: pd.DataFrame,
    start_c: float = 19.0,
    step_c: float = 0.1,
    correction_ref_c: float = 21.0,
) -> None:
    if df_results.empty or df_models.empty:
        print("Skipping LUT export (no results/models).")
        return

    for serial, sub in df_results.groupby('Serial', sort=True):
        sub = sub.dropna(subset=['Temperature'])
        if sub.empty:
            continue

        max_t = float(np.nanmax(sub['Temperature'].astype(float).to_numpy()))
        if not np.isfinite(max_t) or max_t < start_c:
            print(f"Skipping LUT for {serial} (max(T)={max_t:.3f}C < {start_c:.1f}C).")
            continue

        n = int(np.floor((max_t - start_c) / step_c + 1e-9)) + 1
        temps = start_c + step_c * np.arange(n, dtype=float)
        temps = np.round(temps, 1)  # keep grid clean in CSV

        model_row = df_models[df_models['Serial'] == serial]
        if model_row.empty:
            print(f"Skipping LUT for {serial} (no model row).")
            continue
        row = model_row.iloc[0]

        y_fit = _evaluate_model_row(row, temps)
        y_ref = _evaluate_model_row(row, np.array([float(correction_ref_c)], dtype=float))
        ref_val = float(y_ref[0]) if len(y_ref) else np.nan

        lut = pd.DataFrame({
            'Temperature': temps,
            'Avg_Charge_Fit': y_fit,
            f'Correction_to_{_ref_label(float(correction_ref_c))}': y_fit - ref_val,
        })

        out_name = f"calibration_lut_{_safe_filename_fragment(serial)}.csv"
        out_path = dir_path / out_name
        lut.to_csv(out_path, index=False)


# --- Main Execution ---
def build_arg_parser():
    p = argparse.ArgumentParser(description="Charge vs Temperature correlation and per-serial calibration.")
    p.add_argument(
        "--root",
        type=str,
        default=".",
        help="Root directory to search for log files. Default: current directory.",
    )
    p.add_argument(
        "--recursive",
        action="store_true",
        default=True,
        help="Recurse through subdirectories (default: on).",
    )
    p.add_argument(
        "--no-recursive",
        dest="recursive",
        action="store_false",
        help="Do not recurse; only process the --root directory.",
    )
    p.add_argument(
        "--pattern",
        type=str,
        default="Batch*.txt",
        help="Glob pattern for log files within a directory. Default: %(default)s",
    )
    p.add_argument(
        "--exclude-pattern",
        action="append",
        default=["Note*.txt"],
        help=(
            "Glob pattern(s) to exclude within a directory. Can be specified multiple times. "
            "Default: Note*.txt"
        ),
    )
    p.add_argument(
        "--base-date",
        type=str,
        default=None,
        help=(
            "Override injected base date (YYYY-MM-DD). If omitted, the script tries to infer "
            "from filenames like Batch###_YYYY-MM-DD_...txt, otherwise falls back to "
            f"{DEFAULT_BASE_DATE.date().isoformat()}."
        ),
    )
    p.add_argument(
        "--show",
        action="store_true",
        default=False,
        help="Show plots interactively (off by default; recommended off for batch runs).",
    )
    p.add_argument(
        "--fit",
        choices=["poly2", "poly", "smooth"],
        default="smooth",
        help="Fit type per serial. poly2 matches the historical behavior (2nd-order polynomial).",
    )
    p.add_argument(
        "--degree",
        type=int,
        default=FIT_DEGREE,
        help="Polynomial degree when --fit is poly/poly2 (ignored for smooth). Default: %(default)s",
    )
    p.add_argument(
        "--ref-temp",
        type=float,
        default=REF_TEMP_C,
        help="Reference temperature (°C) for normalization. Default: %(default)s",
    )
    p.add_argument(
        "--smooth-k",
        type=int,
        default=3,
        help=(
            "Spline degree for --fit smooth (1..5). Higher k can model more curvature but may "
            "overfit; k=3 (cubic) is usually a good default. Default: %(default)s"
        ),
    )
    p.add_argument(
        "--smooth-s",
        type=float,
        default=None,
        help=(
            "Spline smoothing factor 's' for --fit smooth. s=0 interpolates the points; larger "
            "values produce a smoother curve (less point-to-point wiggle). If omitted, an "
            "automatic s is chosen from data scale and then multiplied by --smooth-s-factor."
        ),
    )
    p.add_argument(
        "--smooth-s-factor",
        type=float,
        default=1.0,
        help=(
            "Multiplier applied only when --smooth-s is not provided. Larger => smoother than "
            "the auto choice; smaller => closer to the data. Default: %(default)s"
        ),
    )
    return p


def _infer_base_date_from_filenames(files: Sequence[Path]) -> Optional[datetime]:
    # Expected: Batch001_2025-12-26_10min.txt (but allow anywhere in name)
    pat = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
    for f in files:
        m = pat.search(f.name)
        if m:
            y, mo, d = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
            try:
                return datetime(y, mo, d)
            except ValueError:
                continue
    return None


def _natural_sort_key(p: Path) -> Tuple:
    # Sort Batch002_... before Batch010_...
    parts = re.split(r'(\d+)', p.name)
    key: List[object] = []
    for part in parts:
        if part.isdigit():
            key.append(int(part))
        else:
            key.append(part.lower())
    return tuple(key)


def _find_log_files_per_directory(root: Path, recursive: bool, pattern: str, exclude_patterns: Sequence[str]) -> Dict[Path, List[Path]]:
    dirs: List[Path]
    if recursive:
        dirs = [p for p in root.rglob("*") if p.is_dir()]
        dirs.insert(0, root)
    else:
        dirs = [root]

    out: Dict[Path, List[Path]] = {}
    for d in dirs:
        matches = list(d.glob(pattern))
        if not matches:
            continue
        if exclude_patterns:
            excluded: set[Path] = set()
            for ex in exclude_patterns:
                excluded.update(d.glob(ex))
            matches = [m for m in matches if m not in excluded]

        if not matches:
            continue

        matches = sorted(matches, key=_natural_sort_key)
        out[d] = matches

    return out


def _run_pipeline_for_directory(
    *,
    dir_path: Path,
    log_files: Sequence[Path],
    base_date: datetime,
    fit_type: str,
    degree: Optional[int],
    ref_temp_c: float,
    smooth_k: int,
    smooth_s: Optional[float],
    smooth_s_factor: float,
    show: bool,
) -> None:
    print(f"\n=== Processing: {dir_path} ===")
    print(f"Found {len(log_files)} log file(s). Base date = {base_date.date().isoformat()}")

    df_charge, df_temp = parse_files(log_files, base_date=base_date)

    df_temp_clean = clean_temperature_data(df_temp)
    df_results = correlate_data(df_charge, df_temp_clean)

    df_results, df_models = fit_and_calibrate_per_serial(
        df_results,
        fit_type=fit_type,
        degree=degree if degree is not None else FIT_DEGREE,
        ref_temp_c=ref_temp_c,
        ref_temps_c=[19.0, 21.0, float(ref_temp_c)],
        smooth_k=int(smooth_k),
        smooth_s=smooth_s,
        smooth_s_factor=float(smooth_s_factor),
    )

    if fit_type == "smooth":
        fit_tag = f"smooth_k{int(smooth_k)}"
    else:
        fit_tag = f"deg{int(degree) if degree is not None else int(FIT_DEGREE)}"

    out_csv = dir_path / f"charge_vs_temp_calibrated_{int(ref_temp_c)}C_{fit_tag}.csv"
    df_results.to_csv(out_csv, index=False)
    print(f"Wrote calibrated results: {out_csv.name}")

    model_csv = dir_path / f"charge_vs_temp_models_{fit_tag}.csv"
    df_models.to_csv(model_csv, index=False)
    print(f"Wrote model table: {model_csv.name}")

    print("Writing per-serial calibration LUTs...")
    _write_per_serial_luts(
        dir_path=dir_path,
        df_results=df_results,
        df_models=df_models,
        start_c=19.0,
        step_c=0.1,
        correction_ref_c=21.0,
    )

    # Plot
    plt.figure(figsize=(10, 6))

    if not df_results.empty and 'Serial' in df_results.columns:
        for serial in df_results['Serial'].unique():
            subset = df_results[df_results['Serial'] == serial].sort_values('Temperature')
            if subset.empty:
                continue

            (line,) = plt.plot(
                subset['Temperature'], subset['Avg_Charge'],
                marker='o', linestyle='-', label=str(serial)
            )

            model_row = df_models[df_models['Serial'] == serial]
            if not model_row.empty:
                row = model_row.iloc[0]
                x_min = float(subset['Temperature'].min())
                x_max = float(subset['Temperature'].max())
                if np.isfinite(x_min) and np.isfinite(x_max) and x_max > x_min:
                    x_line = np.linspace(x_min, x_max, 200)

                    fit_type_row = str(row.get('FitType', 'poly')).strip().lower()
                    y_line = None
                    fit_desc = None

                    if fit_type_row == "smooth" and pd.notna(row.get('SplineT')) and pd.notna(row.get('SplineC')):
                        t = np.array([float(v) for v in str(row['SplineT']).split(',')], dtype=float)
                        c = np.array([float(v) for v in str(row['SplineC']).split(',')], dtype=float)
                        k = int(row.get('SplineK', int(smooth_k)))
                        tck = (t, c, k)
                        y_line = np.asarray(splev(x_line, tck), dtype=float)
                        fit_desc = f"smooth (k={k})"
                    elif pd.notna(row.get('Coeffs_HighToLow')):
                        coeffs = np.array([float(x) for x in str(row['Coeffs_HighToLow']).split(',')], dtype=float)
                        p = np.poly1d(coeffs)
                        y_line = p(x_line)
                        deg = int(row.get('Degree', int(degree) if degree is not None else int(FIT_DEGREE)))
                        fit_desc = f"deg {deg}"

                    if y_line is not None and fit_desc is not None:
                        r2 = float(row['R2']) if pd.notna(row.get('R2')) else None
                        fit_label = f"{serial} fit ({fit_desc}, R²={r2:.3f})" if r2 is not None else f"{serial} fit ({fit_desc})"
                        plt.plot(
                            x_line, y_line,
                            linestyle='--', linewidth=2.0,
                            color=line.get_color(), alpha=0.9,
                            label=fit_label
                        )

    plt.axvline(ref_temp_c, linestyle=':', linewidth=1.5, color='gray', label=f"Ref temp = {ref_temp_c:.0f}°C")
    plt.xlabel('Temperature (°C)')
    plt.ylabel('Average Charge Count')
    title_fit = f"poly deg {int(degree)}" if fit_type == "poly" and degree is not None else f"smoothing spline (k={int(smooth_k)})"
    plt.title(f'Average Charge Count vs Temperature ({dir_path.name}; per-serial {title_fit})')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    out_png = dir_path / f"charge_vs_temp_with_fit_per_serial_{fit_tag}.png"
    plt.savefig(out_png)
    print(f"Saved plot: {out_png.name}")

    if show:
        plt.show()
    plt.close()


def main():
    """
    Run the full pipeline: parse -> clean temps -> correlate -> fit+calibrate -> export -> plot.

    Output filenames are derived from `REF_TEMP_C` and `FIT_DEGREE`:
    - `charge_vs_temp_calibrated_<REF_TEMP_C>C_deg<FIT_DEGREE>.csv`
    - `charge_vs_temp_models_deg<FIT_DEGREE>.csv`
    - `charge_vs_temp_with_fit_per_serial_deg<FIT_DEGREE>.png`
    """
    args = build_arg_parser().parse_args()

    root = Path(args.root).resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"--root must be an existing directory. Got: {root}")

    fit_choice = (args.fit or "poly2").strip().lower()
    if fit_choice == "poly2":
        fit_type = "poly"
        degree = 2
    elif fit_choice == "poly":
        fit_type = "poly"
        degree = int(args.degree)
    else:
        fit_type = "smooth"
        degree = None

    ref_temp_c = float(args.ref_temp)

    # Find log files per directory
    per_dir = _find_log_files_per_directory(
        root=root,
        recursive=bool(args.recursive),
        pattern=str(args.pattern),
        exclude_patterns=list(args.exclude_pattern or []),
    )

    if not per_dir:
        print(f"No log files found under {root} (pattern={args.pattern}).")
        return

    # Resolve base-date override if provided
    base_date_override: Optional[datetime] = None
    if args.base_date:
        try:
            base_date_override = datetime.strptime(str(args.base_date), "%Y-%m-%d")
        except ValueError as e:
            raise SystemExit(f"--base-date must be YYYY-MM-DD. Got: {args.base_date}") from e

    total_dirs = len(per_dir)
    print(f"Discovered {total_dirs} folder(s) with logs under {root}.")

    for dir_path, log_files in sorted(per_dir.items(), key=lambda kv: str(kv[0]).lower()):
        inferred = _infer_base_date_from_filenames(log_files)
        base_date = base_date_override or inferred or DEFAULT_BASE_DATE

        _run_pipeline_for_directory(
            dir_path=dir_path,
            log_files=log_files,
            base_date=base_date,
            fit_type=fit_type,
            degree=degree,
            ref_temp_c=ref_temp_c,
            smooth_k=int(args.smooth_k),
            smooth_s=args.smooth_s,
            smooth_s_factor=float(args.smooth_s_factor),
            show=bool(args.show),
        )

    print("\nAll done.")

if __name__ == "__main__":
    main()