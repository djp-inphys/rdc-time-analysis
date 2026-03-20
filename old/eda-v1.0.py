#!/usr/bin/env python3
"""
eda.py

Exploratory parsing/EDA for CB100 `.txt` batch logs.

### What this script does
- **Discovers** batch log files under a root folder (default: current directory).
- **Parses** reading lines that look like:
  `[13:23:16.906] CB100-2599429--> TS: 1766751796.271 | Pulse: 0 | Charge: 58 | ...`
- **Extracts** per-reading fields (device id, unix timestamp, charge).
- **Builds** two pandas DataFrames:
  - `sessions`: 1 row per (source_file, device_uid), with start/end times.
  - `readings`: 1 row per parsed reading line, with `captured_at` timestamps.
- **Analyzes** reporting frequency (inter-sample time deltas).
- **Optionally exports** per-session readings CSVs.

### Session definition (current)
A “session” is **one device serial across all parsed files**, bounded by the minimum and
maximum `captured_at` timestamps observed for that device across the entire dataset.

We **do not split sessions** due to timestamp gaps between files. However, we do compute
and log **timestamp gaps within each file** (per device) to help identify missing data.

### Usage
Run from repo root:
`python eda.py --root . --pattern "**/*.txt" --filter-date 2025-12-26`

Notes:
- `TS:` in the logs is a unix timestamp with fractional seconds. We interpret it as
  seconds + milliseconds/1000 and convert via `datetime.fromtimestamp(...)` (local time).
  If you want UTC, adjust conversion to `datetime.utcfromtimestamp(...)`.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


LOGGER = logging.getLogger("eda")

# Optional stats backend (used if available).
try:  # pragma: no cover
    from scipy import stats as _spstats  # type: ignore
except Exception:  # pragma: no cover
    _spstats = None


@dataclass(frozen=True)
class CliArgs:
    root: Path
    pattern: str
    filter_date: str
    export_dir: Path
    plots_dir: Path
    gap_csv_dir: Path
    no_plots: bool
    log_level: str
    log_file: Optional[Path]
    gap_threshold_ms: float
    log_gap_details: bool
    show_plots: bool
    temp_bin_size_c: float
    temp_bin_test_alpha: float
    temp_bin_test_min_samples: int
    temp_bin_test_permutations: int


def _write_csv_with_fallback(df: pd.DataFrame, path: Path) -> Path:
    """
    Write CSV to `path`. If the file is locked (PermissionError), write `<stem>_new<suffix>`.

    This is common on Windows if the CSV is currently open in Excel.

    Returns:
        The actual path written.
    """
    try:
        df.to_csv(path, index=False)
        return path
    except PermissionError:
        alt = path.with_name(path.stem + "_new" + path.suffix)
        df.to_csv(alt, index=False)
        return alt


_READING_RE = re.compile(
    r"CB100-(?P<serial>\d+)-->.*?TS:\s*(?P<sec>\d+)\.(?P<ms>\d+)"
    r".*?Pulse:\s*(?P<pulse>\d+).*?Charge:\s*(?P<charge>\d+)",
    re.IGNORECASE,
)

_BRACKET_TIME_RE = re.compile(r"\[(?P<hms>\d{2}:\d{2}:\d{2}\.\d{3})\]")
_TEMP_HEADER_RE = re.compile(r"Temperature Information from\s+(CB100-\d+)\s*:", re.IGNORECASE)
_AMBIENT_TEMP_RE = re.compile(r"Ambient Temperature:\s*([+-]?\d+(?:\.\d+)?)\s*°?C", re.IGNORECASE)
_DATE_IN_NAME_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")


def _iter_txt_files(root_dir: Path, pattern: str) -> Iterable[Path]:
    """
    Yield candidate `.txt` files under `root_dir` matching `pattern`.

    We keep this conservative: only files with "Batch" in the filename are treated
    as data logs to avoid parsing notes.
    """
    for p in root_dir.glob(pattern):
        if not p.is_file():
            continue
        if p.suffix.lower() != ".txt":
            continue
        if "batch" not in p.name.lower():
            continue
        yield p


def _infer_base_date_from_filename(file_name: str) -> Optional[datetime]:
    """
    Infer a calendar date from a filename containing YYYY-MM-DD (e.g. Batch001_2025-12-26_10min.txt).
    """
    m = _DATE_IN_NAME_RE.search(str(file_name))
    if not m:
        return None
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return datetime(y, mo, d)


def _parse_bracket_time(line: str, base_date: datetime) -> Optional[datetime]:
    """
    Parse `[HH:MM:SS.mmm]` from a line and inject the calendar date from `base_date`.
    """
    m = _BRACKET_TIME_RE.search(line)
    if not m:
        return None
    try:
        t = datetime.strptime(m.group("hms"), "%H:%M:%S.%f")
    except ValueError:
        return None
    return base_date.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=t.microsecond)


def _interpolate_temperature_for_readings(
    readings_df: pd.DataFrame, temp_df: pd.DataFrame
) -> pd.Series:
    """
    Interpolate ambient temperature onto readings (best effort), within each (source_file, device_uid).

    Uses `log_time` timestamps for alignment (both readings and temperature records).
    Returns a Series aligned to `readings_df.index` with temperature in °C (float).
    """
    if readings_df.empty or temp_df.empty:
        return pd.Series(index=readings_df.index, dtype=float)
    required_r = {"source_file", "device_uid", "log_time"}
    required_t = {"source_file", "device_uid", "log_time", "temperature_c"}
    if not required_r.issubset(readings_df.columns) or not required_t.issubset(temp_df.columns):
        return pd.Series(index=readings_df.index, dtype=float)

    out = pd.Series(index=readings_df.index, dtype=float)

    # Group within file+device to avoid cross-file gaps and mismatched clocks.
    for (sf, dev), g in readings_df.groupby(["source_file", "device_uid"], sort=False):
        gt = temp_df[(temp_df["source_file"] == sf) & (temp_df["device_uid"] == dev)].copy()
        if gt.empty:
            continue
        g = g.copy()
        # Convert to seconds since epoch for interpolation
        x = pd.to_datetime(g["log_time"], errors="coerce")
        xt = pd.to_datetime(gt["log_time"], errors="coerce")
        m_x = x.notna()
        m_t = xt.notna() & pd.to_numeric(gt["temperature_c"], errors="coerce").notna()
        if m_x.sum() == 0 or m_t.sum() < 2:
            continue

        x_sec = (x[m_x].astype("int64") // 1_000_000_000).to_numpy(dtype=float)
        t_sec = (xt[m_t].astype("int64") // 1_000_000_000).to_numpy(dtype=float)
        y_temp = pd.to_numeric(gt.loc[m_t, "temperature_c"], errors="coerce").to_numpy(dtype=float)

        # Ensure monotonic increasing for np.interp
        order = np.argsort(t_sec)
        t_sec = t_sec[order]
        y_temp = y_temp[order]
        if t_sec.size < 2:
            continue

        interp_vals = np.interp(x_sec, t_sec, y_temp)
        out.loc[g.index[m_x]] = interp_vals

    return out


def parse_txt_files(root_dir: Path, pattern: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parse `.txt` log files and extract reading timestamps and device identifiers.

    Args:
        root_dir: Root folder to search.
        pattern: Glob pattern relative to `root_dir` (e.g. `"**/*.txt"`).

    Returns:
        (sessions_df, readings_df)

        - `readings_df`: one row per parsed reading line with:
          `device_uid`, `captured_at`, `charge_count`, `source_file`, ...
        - `sessions_df`: one row per (source_file, device_uid) containing
          `started_at`/`ended_at` min/max for that device in that file.
    """
    txt_files = sorted(_iter_txt_files(root_dir, pattern), key=lambda p: str(p).lower())
    LOGGER.info("Discovered %d candidate .txt file(s) under %s", len(txt_files), root_dir)

    all_readings: list[dict] = []
    all_temps: list[dict] = []

    for txt_file in txt_files:
        LOGGER.info("Parsing %s", txt_file)
        base_date = _infer_base_date_from_filename(txt_file.name)
        if base_date is None:
            # Fallback: use file mtime date (best effort)
            base_date = datetime.fromtimestamp(txt_file.stat().st_mtime)
            base_date = base_date.replace(hour=0, minute=0, second=0, microsecond=0)
        try:
            with open(txt_file, "r", encoding="utf-8", errors="ignore") as f:
                # Read all lines once to allow lookahead for temperature blocks.
                lines = list(f)
                i = 0
                while i < len(lines):
                    line = lines[i].strip()

                    # Temperature blocks:
                    # [HH:MM:SS.mmm] Temperature Information from CB100-XXXX:
                    # Ambient Temperature: 26.65°C
                    th = _TEMP_HEADER_RE.search(line)
                    if th:
                        dev = th.group(1)
                        t_log = _parse_bracket_time(line, base_date)
                        temp_val = None
                        for off in range(1, 5):
                            if i + off >= len(lines):
                                break
                            m_temp = _AMBIENT_TEMP_RE.search(lines[i + off])
                            if m_temp:
                                try:
                                    temp_val = float(m_temp.group(1))
                                except ValueError:
                                    temp_val = None
                                break
                        if t_log is not None and temp_val is not None:
                            all_temps.append(
                                {
                                    "source_file": txt_file.name,
                                    "device_uid": dev,
                                    "log_time": t_log,
                                    "temperature_c": temp_val,
                                }
                            )
                        i += 1
                        continue

                    # Reading lines
                    m = _READING_RE.search(line)
                    if not m:
                        i += 1
                        continue

                    device_serial = f"CB100-{m.group('serial')}"
                    ts_seconds = int(m.group("sec"))
                    ts_milliseconds = int(m.group("ms"))
                    charge_count = int(m.group("charge"))

                    # Convert unix timestamp (seconds + fractional milliseconds) to datetime.
                    # NOTE: `fromtimestamp` uses local timezone.
                    timestamp = datetime.fromtimestamp(ts_seconds + ts_milliseconds / 1000.0)
                    log_time = _parse_bracket_time(line, base_date)

                    all_readings.append(
                        {
                            "device_uid": device_serial,
                            "dosimeter_label": device_serial,  # keep notebook-compatible name
                            "captured_at": timestamp,
                            "log_time": log_time,
                            "created_at": timestamp,  # same as captured_at for logs
                            "charge_count": charge_count,
                            "source_file": txt_file.name,
                        }
                    )
                    i += 1
        except OSError:
            LOGGER.exception("Failed reading %s", txt_file)

    if not all_readings:
        LOGGER.warning("No readings parsed. Check --root/--pattern and file formats.")
        return pd.DataFrame(), pd.DataFrame()

    readings_df = pd.DataFrame(all_readings)
    readings_df["captured_at"] = pd.to_datetime(readings_df["captured_at"], errors="coerce")
    if "log_time" in readings_df.columns:
        readings_df["log_time"] = pd.to_datetime(readings_df["log_time"], errors="coerce")
    readings_df["created_at"] = pd.to_datetime(readings_df["created_at"], errors="coerce")

    # Extract a convenient numeric key like the notebook did (last 3 digits of the serial).
    # This is only for grouping/plot labeling; it is NOT an inherent dosimeter identity.
    readings_df["dosimeter_num"] = pd.to_numeric(
        readings_df["dosimeter_label"].astype(str).str[-3:], errors="coerce"
    ).astype("Int64")

    # Log intra-file timestamp gaps (per device) for debugging missing samples.
    # We compute this before session construction so we can group by source_file.
    # Default threshold chosen to avoid spamming logs for normal ~200ms cadence.
    # (Can be overridden via CLI.)
    # NOTE: we sort by captured_at within each (source_file, device_uid) stream.
    #
    # This function is defined below; called here to keep parse_txt_files as the single
    # point that produces the canonical `readings_df`.
    #
    # (We guard on column existence/empties inside the helper.)
    #
    # This is a no-op until `main()` passes CLI options (kept backwards compatible).
    # See: `main()` calling `parse_txt_files(...)` then `log_intra_file_gaps(...)`.

    # Session model: one session per device serial across all files, bounded by min/max timestamps.
    sessions_list: list[dict] = []
    session_id_counter = 1

    for device in sorted(readings_df["device_uid"].dropna().unique()):
        device_readings = readings_df[readings_df["device_uid"] == device].sort_values("captured_at")
        if device_readings.empty:
            continue
        started_at = device_readings["captured_at"].min()
        ended_at = device_readings["captured_at"].max()
        sessions_list.append(
            {
                "id": session_id_counter,
                "started_at": started_at,
                "ended_at": ended_at,
                "device_uid": device,
                "n_readings": int(len(device_readings)),
                "n_files": int(device_readings["source_file"].nunique(dropna=True)),
            }
        )
        session_id_counter += 1

    sessions_df = pd.DataFrame(sessions_list)
    if not sessions_df.empty:
        sessions_df["started_at"] = pd.to_datetime(sessions_df["started_at"], errors="coerce")
        sessions_df["ended_at"] = pd.to_datetime(sessions_df["ended_at"], errors="coerce")

    # Assign monitoring_session_id to each reading by device serial (global session).
    readings_df["monitoring_session_id"] = pd.NA
    if not sessions_df.empty:
        dev_to_id = dict(zip(sessions_df["device_uid"], sessions_df["id"]))
        readings_df["monitoring_session_id"] = readings_df["device_uid"].map(dev_to_id)

    # Add a simple row id (DB-like) for convenience.
    readings_df["id"] = range(1, len(readings_df) + 1)

    # Temperature interpolation (best effort)
    if all_temps:
        temp_df = pd.DataFrame(all_temps)
        temp_df["log_time"] = pd.to_datetime(temp_df["log_time"], errors="coerce")
        temp_df["temperature_c"] = pd.to_numeric(temp_df["temperature_c"], errors="coerce")
        readings_df["temperature_c"] = _interpolate_temperature_for_readings(readings_df, temp_df)
        LOGGER.info(
            "Temperature records: %d (devices=%d, files=%d)",
            len(temp_df),
            temp_df["device_uid"].nunique(dropna=True),
            temp_df["source_file"].nunique(dropna=True),
        )
    else:
        readings_df["temperature_c"] = np.nan

    LOGGER.info("Parsed %d readings across %d session(s)", len(readings_df), len(sessions_df))
    LOGGER.info("Devices found: %d", readings_df["device_uid"].nunique(dropna=True))
    LOGGER.info(
        "Date range: %s to %s", readings_df["captured_at"].min(), readings_df["captured_at"].max()
    )

    return sessions_df, readings_df


def setup_logging(level: str, log_file: Optional[Path] = None) -> None:
    """
    Configure standard library logging for this script.
    """
    numeric = getattr(logging, str(level).upper(), None)
    if not isinstance(numeric, int):
        numeric = logging.DEBUG

    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=numeric,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
        force=True,  # override any prior logging config (common in notebooks/IDEs)
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Define CLI flags for repeatable analysis runs.
    """
    p = argparse.ArgumentParser(description="Parse CB100 batch logs and perform timestamp EDA.")
    p.add_argument(
        "--root",
        type=str,
        default=".",
        help="Root directory to search for batch .txt files (default: current directory).",
    )
    p.add_argument(
        "--pattern",
        type=str,
        default="**/*.txt",
        help='Glob pattern under --root (default: "**/*.txt").',
    )
    p.add_argument(
        "--filter-date",
        type=str,
        default="all",
        help='Restrict analysis to a date "YYYY-MM-DD", or "all" for full dataset (default: %(default)s).',
    )
    p.add_argument(
        "--export-dir",
        type=str,
        default="session_exports",
        help="Output folder for CSV exports (default: %(default)s).",
    )
    p.add_argument(
        "--plots-dir",
        type=str,
        default="plots",
        help="Output folder for plots (one PNG per device). Default: %(default)s.",
    )
    p.add_argument(
        "--gap-csv-dir",
        type=str,
        default="gap_exports",
        help="Output folder for within-file gap CSV exports (one CSV per serial). Default: %(default)s.",
    )
    p.add_argument(
        "--no-plots",
        action="store_true",
        default=False,
        help="Disable matplotlib plots (useful for headless runs).",
    )
    p.add_argument(
        "--show-plots",
        action="store_true",
        default=False,
        help="Show plots interactively (in addition to saving PNGs).",
    )
    p.add_argument(
        "--temp-bin-size-c",
        type=float,
        default=5.0,
        help="Temperature bin size in °C for the Δt candle-by-temperature plots. Default: %(default)s.",
    )
    p.add_argument(
        "--temp-bin-test-alpha",
        type=float,
        default=0.05,
        help="Alpha for temperature-bin significance tests (BH-FDR on adjacent bin comparisons). Default: %(default)s.",
    )
    p.add_argument(
        "--temp-bin-test-min-samples",
        type=int,
        default=200,
        help="Minimum samples required per bin to test adjacent bin differences. Default: %(default)s.",
    )
    p.add_argument(
        "--temp-bin-test-permutations",
        type=int,
        default=2000,
        help="Permutation count for median-difference tests between adjacent bins. Default: %(default)s.",
    )
    p.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: %(default)s.",
    )
    p.add_argument(
        "--log-file",
        type=str,
        default="eda.log",
        help="Optional path to write logs to (in addition to console). Example: eda.log",
    )
    p.add_argument(
        "--gap-threshold-ms",
        type=float,
        default=1000.0,
        help=(
            "Log intra-file timestamp gaps (per device) when delta exceeds this threshold in ms. "
            "Default: %(default)s"
        ),
    )
    p.add_argument(
        "--log-gap-details",
        action="store_true",
        default=True,
        help="If set, log individual gap occurrences (can be verbose).",
    )
    return p


def _parse_args(argv: Optional[list[str]] = None) -> CliArgs:
    """
    Parse CLI arguments.

    Args:
        argv: Optional argv list for programmatic calls/tests. If None, argparse uses sys.argv.
    """
    p = build_arg_parser()
    args = p.parse_args(argv)
    return CliArgs(
        root=Path(args.root).resolve(),
        pattern=str(args.pattern),
        filter_date=str(args.filter_date),
        export_dir=Path(args.export_dir),
        plots_dir=Path(args.plots_dir),
        gap_csv_dir=Path(args.gap_csv_dir),
        no_plots=bool(args.no_plots),
        log_level=str(args.log_level),
        log_file=(Path(args.log_file).resolve() if args.log_file else None),
        gap_threshold_ms=float(args.gap_threshold_ms),
        log_gap_details=bool(args.log_gap_details),
        show_plots=bool(args.show_plots),
        temp_bin_size_c=float(args.temp_bin_size_c),
        temp_bin_test_alpha=float(args.temp_bin_test_alpha),
        temp_bin_test_min_samples=int(args.temp_bin_test_min_samples),
        temp_bin_test_permutations=int(args.temp_bin_test_permutations),
    )


def log_intra_file_gaps(
    readings_df: pd.DataFrame,
    threshold_ms: float = 1000.0,
    log_details: bool = False,
) -> None:
    """
    Log timestamp gaps within each file (per device).

    We ignore gaps across files for session construction, but gaps *within* a file can indicate
    missing samples or pauses.

    Args:
        readings_df: Parsed readings (must include `source_file`, `device_uid`, `captured_at`).
        threshold_ms: Only gaps larger than this threshold are summarized at WARNING level.
        log_details: If True, logs each gap occurrence (at INFO) for gaps > threshold_ms.
    """
    if readings_df.empty:
        return
    required = {"source_file", "device_uid", "captured_at"}
    if not required.issubset(set(readings_df.columns)):
        return

    # Ensure datetime
    ts = pd.to_datetime(readings_df["captured_at"], errors="coerce")
    df = readings_df.copy()
    df["captured_at"] = ts
    df = df.dropna(subset=["captured_at", "source_file", "device_uid"])
    if df.empty:
        return

    for (source_file, device_uid), g in df.groupby(["source_file", "device_uid"], sort=True):
        g = g.sort_values("captured_at")
        diffs = g["captured_at"].diff()
        diffs_ms = diffs.dt.total_seconds().mul(1000.0)
        diffs_ms = diffs_ms.dropna()
        if diffs_ms.empty:
            continue

        big = diffs_ms[diffs_ms > float(threshold_ms)]
        if big.empty:
            LOGGER.debug(
                "No intra-file gaps > %.1fms for file=%s device=%s (max=%.1fms)",
                float(threshold_ms),
                source_file,
                device_uid,
                float(diffs_ms.max()),
            )
            continue

        max_gap = float(big.max())
        LOGGER.warning(
            "Intra-file timestamp gaps detected: file=%s device=%s count=%d max=%.1fms (threshold=%.1fms)",
            source_file,
            device_uid,
            int(big.shape[0]),
            max_gap,
            float(threshold_ms),
        )

        if log_details:
            # Log a limited number at INFO to avoid pathological spam.
            # If you need all gaps, run with --log-level DEBUG and remove this cap.
            cap = 50
            idxs = list(big.index[:cap])
            for ix in idxs:
                prev_ts = g.loc[ix, "captured_at"] - diffs.loc[ix]
                cur_ts = g.loc[ix, "captured_at"]
                LOGGER.info(
                    "  gap=%.1fms file=%s device=%s prev=%s curr=%s",
                    float(big.loc[ix]),
                    source_file,
                    device_uid,
                    prev_ts,
                    cur_ts,
                )
            if big.shape[0] > cap:
                LOGGER.info(
                    "  (truncated) %d more gap(s) not shown for file=%s device=%s",
                    int(big.shape[0] - cap),
                    source_file,
                    device_uid,
                )


def write_intra_file_gap_csvs(
    readings_df: pd.DataFrame,
    out_dir: Path,
    filter_tag: str,
    threshold_ms: float,
) -> list[Path]:
    """
    Write within-file timestamp deltas (Δt) to one CSV per device serial.

    This exports only within-file deltas (Δt) that exceed `threshold_ms`. Each row
    represents a gap between two consecutive readings within the SAME `source_file`, so
    between-file gaps are intentionally excluded.

    Output filename pattern:
        <filter_tag>_gaps_device_<serial>.csv

    Args:
        readings_df: Parsed readings (must include `source_file`, `device_uid`, `captured_at`).
        out_dir: Directory to write per-device gap CSVs.
        filter_tag: Tag used in filenames (e.g. "all" or "YYYY-MM-DD").
        threshold_ms: Threshold used elsewhere for logging; included here as a boolean flag.

    Returns:
        List of paths written (actual path may differ if file was locked and fallback name used).
    """
    if readings_df.empty:
        return []
    required = {"source_file", "device_uid", "captured_at"}
    if not required.issubset(set(readings_df.columns)):
        return []

    out_dir.mkdir(parents=True, exist_ok=True)

    df = readings_df.copy()
    df["captured_at"] = pd.to_datetime(df["captured_at"], errors="coerce")
    df = df.dropna(subset=["captured_at", "source_file", "device_uid"])
    if df.empty:
        return []

    written_paths: list[Path] = []

    for device_uid, gd in df.groupby("device_uid", sort=True):
        rows: list[dict] = []

        for source_file, gf in gd.groupby("source_file", sort=True):
            g = gf.sort_values("captured_at").reset_index(drop=True)
            if len(g) < 2:
                continue

            delta_ms = g["captured_at"].diff().dt.total_seconds().mul(1000.0)
            prev_ts = g["captured_at"].shift(1)

            for i in range(1, len(g)):
                dms = delta_ms.iat[i]
                if pd.isna(dms):
                    continue
                if float(dms) <= float(threshold_ms):
                    continue

                rec: dict = {
                    "device_uid": device_uid,
                    "serial": str(device_uid).split("-")[-1],
                    "source_file": source_file,
                    "prev_captured_at": prev_ts.iat[i],
                    "captured_at": g["captured_at"].iat[i],
                    "delta_ms": float(dms),
                    "gap_exceeds_threshold": True,
                    "threshold_ms": float(threshold_ms),
                    "row_in_file": int(i + 1),  # 1-based row number in the sorted file subset
                }

                if "id" in g.columns:
                    rec["prev_id"] = g["id"].iat[i - 1]
                    rec["id"] = g["id"].iat[i]
                if "charge_count" in g.columns:
                    rec["prev_charge_count"] = g["charge_count"].iat[i - 1]
                    rec["charge_count"] = g["charge_count"].iat[i]
                if "monitoring_session_id" in g.columns:
                    rec["monitoring_session_id"] = g["monitoring_session_id"].iat[i]
                if "dosimeter_label" in g.columns:
                    rec["dosimeter_label"] = g["dosimeter_label"].iat[i]
                if "created_at" in g.columns:
                    rec["created_at"] = g["created_at"].iat[i]

                rows.append(rec)

        if not rows:
            continue

        gaps_df = pd.DataFrame(rows)
        gaps_df = gaps_df.sort_values(["source_file", "captured_at"], kind="mergesort")

        serial_digits = str(device_uid).split("-")[-1]
        out_path = out_dir / (
            f"{_sanitize_filename_fragment(filter_tag)}_gaps_device_{_sanitize_filename_fragment(serial_digits)}.csv"
        )
        written = _write_csv_with_fallback(gaps_df, out_path)
        if written != out_path:
            LOGGER.warning("Gap CSV target was locked; wrote instead: %s", written)
        LOGGER.info("Exported intra-file gaps: %s (rows=%d)", written, int(len(gaps_df)))
        written_paths.append(written)

    return written_paths


def _sanitize_filename_fragment(s: str) -> str:
    """
    Make a safe filename fragment for Windows paths.
    """
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(s)).strip("_")


def _bh_fdr(p_values: np.ndarray) -> np.ndarray:
    """
    Benjamini–Hochberg FDR adjustment.

    Args:
        p_values: array of p-values in [0, 1]

    Returns:
        q-values (same shape) controlling FDR.
    """
    p = np.asarray(p_values, dtype=float)
    n = int(p.size)
    if n == 0:
        return p
    order = np.argsort(p)
    ranked = p[order]
    q = ranked * n / (np.arange(n, dtype=float) + 1.0)
    # enforce monotonicity from largest to smallest
    q = np.minimum.accumulate(q[::-1])[::-1]
    q = np.clip(q, 0.0, 1.0)
    out = np.empty_like(q)
    out[order] = q
    return out


def _permutation_test_median(
    a: np.ndarray,
    b: np.ndarray,
    n_perm: int = 2000,
    seed: int = 0,
) -> float:
    """
    Two-sided permutation test for difference in medians.

    This is dependency-free and robust for skewed distributions (like Δt with occasional dropouts).
    Returns an approximate p-value.
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    a = a[np.isfinite(a)]
    b = b[np.isfinite(b)]
    if a.size < 2 or b.size < 2:
        return float("nan")

    obs = float(np.median(a) - np.median(b))
    pooled = np.concatenate([a, b])
    n_a = int(a.size)

    rng = np.random.default_rng(seed)
    more_extreme = 0
    for _ in range(int(n_perm)):
        perm = rng.permutation(pooled)
        aa = perm[:n_a]
        bb = perm[n_a:]
        stat = float(np.median(aa) - np.median(bb))
        if abs(stat) >= abs(obs):
            more_extreme += 1

    # add-one smoothing
    return float((more_extreme + 1) / (int(n_perm) + 1))


def select_analysis_readings(readings_df: pd.DataFrame, filter_date: str) -> pd.DataFrame:
    """
    Choose which subset of readings to analyze/plot/export based on --filter-date.

    Args:
        readings_df: Parsed readings.
        filter_date: "all" or "YYYY-MM-DD".

    Returns:
        A DataFrame view/copy containing the readings used for analysis.
    """
    if readings_df.empty:
        return readings_df
    if not filter_date or str(filter_date).strip().lower() in {"all", "*"}:
        return readings_df
    ts = pd.to_datetime(readings_df["captured_at"], errors="coerce")
    m = ts.dt.strftime("%Y-%m-%d") == str(filter_date).strip()
    return readings_df[m].copy()


def add_gateway_device_lag(readings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute lag between gateway log time and device timestamp.

    Lag definition:
        lag_seconds = log_time - captured_at

    Where:
    - `log_time` comes from the gateway log line prefix `[HH:MM:SS.mmm]` (with file date injected)
    - `captured_at` comes from the device `TS:` field (unix timestamp in seconds with fractional ms)

    Returns:
        A DataFrame with `lag_seconds` (float) added when possible.
    """
    if readings_df.empty:
        return readings_df
    if "log_time" not in readings_df.columns or "captured_at" not in readings_df.columns:
        return readings_df

    df = readings_df.copy()
    lt = pd.to_datetime(df["log_time"], errors="coerce")
    ct = pd.to_datetime(df["captured_at"], errors="coerce")
    df["lag_seconds"] = (lt - ct).dt.total_seconds()
    return df


def make_device_lag_plots(
    readings_df: pd.DataFrame,
    plots_dir: Path,
    show: bool = False,
    max_points: int = 20000,
) -> None:
    """
    For each device serial, save a Lag vs Time plot:

        lag_seconds = log_time - captured_at

    This helps visualize:
    - Offset at start (y-intercept)
    - Drift (slope) consistent with oscillator thermal drift
    - Jitter/latency (thickness/spikes)
    """
    if readings_df.empty:
        return
    required = {"device_uid", "log_time", "lag_seconds"}
    if not required.issubset(set(readings_df.columns)):
        return

    plots_dir.mkdir(parents=True, exist_ok=True)

    for device_uid, g in readings_df.groupby("device_uid", sort=True):
        if g.empty:
            continue

        x = pd.to_datetime(g["log_time"], errors="coerce")
        y = pd.to_numeric(g["lag_seconds"], errors="coerce")
        m = x.notna() & y.notna()
        if int(m.sum()) < 2:
            continue

        gg = pd.DataFrame({"log_time": x[m], "lag_seconds": y[m]}).sort_values("log_time")

        if len(gg) > int(max_points):
            gg_plot = gg.sample(n=int(max_points), random_state=0).sort_values("log_time")
            LOGGER.info("Downsampling lag plot: device=%s %d -> %d points", device_uid, len(gg), len(gg_plot))
        else:
            gg_plot = gg

        # Estimate drift slope (seconds of lag change per hour) for annotation.
        slope_s_per_hr = None
        try:
            t0 = gg["log_time"].iloc[0]
            dt_s = (gg["log_time"] - t0).dt.total_seconds().to_numpy(dtype=float)
            lag_s = gg["lag_seconds"].to_numpy(dtype=float)
            ok = np.isfinite(dt_s) & np.isfinite(lag_s)
            if int(ok.sum()) >= 2:
                slope, intercept = np.polyfit(dt_s[ok], lag_s[ok], deg=1)
                slope_s_per_hr = float(slope) * 3600.0
        except Exception:
            slope_s_per_hr = None

        fig, ax = plt.subplots(figsize=(12, 4))
        ax.plot(gg_plot["log_time"], gg_plot["lag_seconds"], linewidth=0.8)
        ax.set_title(f"{device_uid} - Lag vs Time (gateway - device)")
        ax.set_xlabel("Gateway log time")
        ax.set_ylabel("Lag (seconds)")
        ax.grid(True, alpha=0.3)

        if slope_s_per_hr is not None:
            ax.text(
                0.02,
                0.98,
                f"drift≈{slope_s_per_hr:+.3f} s/hour",
                transform=ax.transAxes,
                va="top",
                ha="left",
                fontsize=9,
                family="monospace",
            )

        fig.tight_layout()

        out_name = f"{_sanitize_filename_fragment(device_uid)}_lag_vs_time.png"
        out_path = plots_dir / out_name
        try:
            fig.savefig(out_path, dpi=150)
        except PermissionError:
            alt = out_path.with_name(out_path.stem + "_new" + out_path.suffix)
            fig.savefig(alt, dpi=150)
            out_path = alt
        LOGGER.info("Saved lag plot: %s", out_path)

        if not show:
            plt.close(fig)


def within_file_deltas_ms(readings_one_device: pd.DataFrame) -> pd.Series:
    """
    Compute inter-sample Δt (ms) for a single device, **excluding between-file gaps**.

    This computes `.diff()` within each `source_file` group (sorted by `captured_at`),
    then concatenates all intra-file deltas.
    """
    if readings_one_device.empty:
        return pd.Series(dtype=float)
    if "captured_at" not in readings_one_device.columns or "source_file" not in readings_one_device.columns:
        return pd.Series(dtype=float)

    out: list[pd.Series] = []
    for _, gf in readings_one_device.groupby("source_file", sort=False):
        ts = pd.to_datetime(gf["captured_at"], errors="coerce").sort_values()
        d = ts.diff().dt.total_seconds().mul(1000.0).dropna()
        if not d.empty:
            out.append(d.astype(float))

    if not out:
        return pd.Series(dtype=float)
    return pd.concat(out, ignore_index=True)


def make_device_plots(
    readings_df: pd.DataFrame,
    plots_dir: Path,
    show: bool = False,
    max_points: int = 20000,
    dt_max_ms: Optional[float] = None,
) -> None:
    """
    For each device serial, save:
    - **Time vs Index** plot (x=time, y=index)
    - **Candlestick-style Δt summary** (box/whisker “candle” of inter-sample deltas)
    - **Charge vs Time** plot (x=time, y=charge_count)

    Note:
        Δt statistics are computed **within each source file** and aggregated across files.
        This intentionally **excludes between-file gaps**.

    Args:
        readings_df: Analysis readings (possibly date-filtered).
        plots_dir: Directory where PNGs will be written.
        show: If True, show interactively as real Matplotlib figures (pan/zoom works).
        max_points: Downsample each device time-series if larger than this many points (plot only).
        dt_max_ms: If provided, exclude Δt values larger than this threshold (ms) from the candle.
            This is useful to ignore dropouts/pauses while still logging them elsewhere.
    """
    if readings_df.empty:
        LOGGER.warning("No readings available for plotting.")
        return

    plots_dir.mkdir(parents=True, exist_ok=True)

    for device_uid, g in readings_df.groupby("device_uid", sort=True):
        g = g.sort_values("captured_at")
        if g.empty:
            continue

        # Build index series
        idx = pd.Series(range(len(g)), index=g.index)

        # Optional downsample for responsiveness (plot only; stats use full data)
        if len(g) > int(max_points):
            g_plot = g.sample(n=int(max_points), random_state=0).sort_values("captured_at")
            idx_plot = pd.Series(range(len(g_plot)), index=g_plot.index)
            LOGGER.info("Downsampling for plot: device=%s %d -> %d points", device_uid, len(g), len(g_plot))
        else:
            g_plot = g
            idx_plot = idx

        # Δt computed WITHIN each file only (exclude between-file gaps).
        deltas_ms_all = within_file_deltas_ms(g)
        if dt_max_ms is not None and not deltas_ms_all.empty:
            deltas_ms = deltas_ms_all[deltas_ms_all <= float(dt_max_ms)]
            n_excluded = int((deltas_ms_all > float(dt_max_ms)).sum())
        else:
            deltas_ms = deltas_ms_all
            n_excluded = 0

        fig, axes = plt.subplots(ncols=3, figsize=(20, 5))
        ax_time, ax_hist, ax_charge = axes

        # 1) time vs index
        ax_time.plot(
            pd.to_datetime(g_plot["captured_at"], errors="coerce"),
            idx_plot.to_numpy(),
            linewidth=0.8,
        )
        ax_time.set_title(f"{device_uid} - Time vs Index")
        ax_time.set_xlabel("Time")
        ax_time.set_ylabel("Index")
        ax_time.grid(True, alpha=0.3)

        # 2) candle of deltas (frequency proxy)
        if deltas_ms.empty:
            ax_hist.text(0.5, 0.5, "No deltas", ha="center", va="center", transform=ax_hist.transAxes)
        else:
            # Candlestick-style summary via a boxplot:
            # whiskers ~ min/max (or 1.5 IQR depending on Matplotlib settings),
            # box = Q1..Q3, line = median. This is a compact "candle" for Δt distribution.
            vals = deltas_ms.to_numpy(dtype=float)
            ax_hist.boxplot(
                [vals],
                vert=True,
                widths=0.35,
                showfliers=True,
                patch_artist=True,
                boxprops={"facecolor": "lightgray", "edgecolor": "black"},
                medianprops={"color": "red", "linewidth": 1.5},
                whiskerprops={"color": "black"},
                capprops={"color": "black"},
                flierprops={"marker": "o", "markersize": 2, "alpha": 0.25, "markerfacecolor": "black"},
            )
            ax_hist.set_title(f"{device_uid} - Δt Candle (ms) [ignores between-file gaps]")
            ax_hist.set_ylabel("Δt (ms)")
            ax_hist.set_xticks([1])
            ax_hist.set_xticklabels(["Δt"])
            ax_hist.grid(True, alpha=0.3, axis="y")

            # Add a tiny stats annotation (median + mean) for quick reading.
            med = float(pd.Series(vals).median())
            mean = float(pd.Series(vals).mean())
            ax_hist.text(
                0.02,
                0.98,
                (
                    f"median={med:.1f}ms\n"
                    f"mean={mean:.1f}ms\n"
                    f"n={len(vals)}"
                    + (f"\nexcluded>{dt_max_ms:.0f}ms={n_excluded}" if (dt_max_ms is not None and n_excluded) else "")
                ),
                transform=ax_hist.transAxes,
                va="top",
                ha="left",
                fontsize=9,
                family="monospace",
            )

        # 3) charge vs time
        if "charge_count" not in g_plot.columns:
            ax_charge.text(
                0.5,
                0.5,
                "No charge_count",
                ha="center",
                va="center",
                transform=ax_charge.transAxes,
            )
        else:
            t = pd.to_datetime(g_plot["captured_at"], errors="coerce")
            y = pd.to_numeric(g_plot["charge_count"], errors="coerce")
            m = t.notna() & y.notna()
            if int(m.sum()) == 0:
                ax_charge.text(
                    0.5,
                    0.5,
                    "No valid charge/time",
                    ha="center",
                    va="center",
                    transform=ax_charge.transAxes,
                )
            else:
                ax_charge.plot(t[m], y[m], linewidth=0.8)
                ax_charge.set_title(f"{device_uid} - Charge vs Time")
                ax_charge.set_xlabel("Time")
                ax_charge.set_ylabel("Charge")
                ax_charge.grid(True, alpha=0.3)

        date_min = pd.to_datetime(g["captured_at"], errors="coerce").min()
        date_max = pd.to_datetime(g["captured_at"], errors="coerce").max()
        fig.suptitle(f"{device_uid}  ({date_min} .. {date_max})")
        fig.tight_layout()

        out_name = f"{_sanitize_filename_fragment(device_uid)}_time_index_dt_candle_and_charge.png"
        out_path = plots_dir / out_name
        try:
            fig.savefig(out_path, dpi=150)
        except PermissionError:
            alt = out_path.with_name(out_path.stem + "_new" + out_path.suffix)
            fig.savefig(alt, dpi=150)
            out_path = alt
        LOGGER.info("Saved plot: %s", out_path)

        # If showing interactively, keep the real figure open so pan/zoom works.
        # We'll call `plt.show()` once after generating all figures.
        if not show:
            plt.close(fig)

    if show:
        plt.show()


def make_device_temp_bin_candle_plots(
    readings_df: pd.DataFrame,
    plots_dir: Path,
    temp_bin_size_c: float = 5.0,
    dt_max_ms: Optional[float] = None,
    stats_alpha: float = 0.05,
    stats_min_samples: int = 200,
    stats_n_permutations: int = 2000,
    show: bool = False,
) -> None:
    """
    Create per-device Δt candle plots broken down by ambient temperature bins.

    Temperature is taken from `readings_df["temperature_c"]` (interpolated from log temperature blocks).
    Bins are computed as floor(temp/bin_size)*bin_size .. +bin_size.

    Δt samples are computed within each file and aggregated across files (between-file gaps excluded).
    We then assign each Δt sample to the temperature bin of the *current* reading.

    Output:
        One PNG per device: `<device>_dt_candle_by_temp.png`
    """
    if readings_df.empty or "temperature_c" not in readings_df.columns:
        LOGGER.warning("No temperature data available for temp-binned plots.")
        return

    plots_dir.mkdir(parents=True, exist_ok=True)
    bin_size = float(temp_bin_size_c) if temp_bin_size_c and temp_bin_size_c > 0 else 5.0

    for device_uid, g in readings_df.groupby("device_uid", sort=True):
        g = g.sort_values("captured_at").copy()
        if g.empty:
            continue

        temps = pd.to_numeric(g["temperature_c"], errors="coerce")
        if temps.notna().sum() < 5:
            LOGGER.info("Skipping temp-binned candle for %s (insufficient temperature samples)", device_uid)
            continue

        dt_series, temp_series = collect_dt_and_temp_samples(
            readings_one_device=g,
            dt_max_ms=dt_max_ms,
        )

        if dt_series.empty or temp_series.empty:
            continue

        # Bin temperatures
        bin_low = (np.floor(temp_series / bin_size) * bin_size).astype(float)
        bins = sorted(bin_low.unique())
        if not bins:
            continue

        # Significance tests are written as a single combined CSV from `main()`.

        # Prepare subplots
        n = len(bins)
        ncols = 3
        nrows = int(np.ceil(n / ncols))
        fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(5.5 * ncols, 3.8 * nrows), sharey=True)
        axes = np.atleast_1d(axes).reshape(nrows, ncols)

        for j, b0 in enumerate(bins):
            r = j // ncols
            c = j % ncols
            ax = axes[r, c]
            b1 = b0 + bin_size
            m_bin = (bin_low == b0)
            vals = dt_series[m_bin].to_numpy(dtype=float)
            if vals.size == 0:
                ax.set_axis_off()
                continue

            ax.boxplot(
                [vals],
                vert=True,
                widths=0.35,
                showfliers=True,
                patch_artist=True,
                boxprops={"facecolor": "lightgray", "edgecolor": "black"},
                medianprops={"color": "red", "linewidth": 1.5},
                whiskerprops={"color": "black"},
                capprops={"color": "black"},
                flierprops={"marker": "o", "markersize": 2, "alpha": 0.25, "markerfacecolor": "black"},
            )
            ax.set_title(f"{b0:.0f}–{b1:.0f}°C  (n={vals.size})")
            ax.set_xticks([1])
            ax.set_xticklabels(["Δt"])
            ax.grid(True, alpha=0.3, axis="y")

        # Hide unused axes
        for k in range(n, nrows * ncols):
            r = k // ncols
            c = k % ncols
            axes[r, c].set_axis_off()

        fig.suptitle(f"{device_uid} - Δt Candle by Temperature Bin ({bin_size:.0f}°C)")
        fig.tight_layout()

        out_path = plots_dir / f"{_sanitize_filename_fragment(device_uid)}_dt_candle_by_temp.png"
        try:
            fig.savefig(out_path, dpi=150)
        except PermissionError:
            alt = out_path.with_name(out_path.stem + "_new" + out_path.suffix)
            fig.savefig(alt, dpi=150)
            out_path = alt
        LOGGER.info("Saved temp-binned plot: %s", out_path)
        if not show:
            plt.close(fig)

    if show:
        plt.show()


def collect_dt_and_temp_samples(
    readings_one_device: pd.DataFrame,
    dt_max_ms: Optional[float],
) -> tuple[pd.Series, pd.Series]:
    """
    For a single device, collect Δt (ms) samples and corresponding temperature samples.

    - Δt is computed within each source file (between-file gaps excluded).
    - Temperature is taken from `temperature_c` at the current sample.
    - If dt_max_ms is provided, exclude Δt > dt_max_ms.
    """
    if readings_one_device.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float)
    if "temperature_c" not in readings_one_device.columns:
        return pd.Series(dtype=float), pd.Series(dtype=float)
    if "captured_at" not in readings_one_device.columns or "source_file" not in readings_one_device.columns:
        return pd.Series(dtype=float), pd.Series(dtype=float)

    dt_vals: list[float] = []
    dt_temps: list[float] = []
    for _, gf in readings_one_device.groupby("source_file", sort=False):
        gf = gf.sort_values("captured_at")
        t = pd.to_datetime(gf["captured_at"], errors="coerce")
        dt = t.diff().dt.total_seconds().mul(1000.0)
        temp_here = pd.to_numeric(gf["temperature_c"], errors="coerce")
        m = dt.notna() & temp_here.notna()
        if dt_max_ms is not None:
            m = m & (dt <= float(dt_max_ms))
        if m.sum() == 0:
            continue
        dt_vals.extend(dt[m].astype(float).tolist())
        dt_temps.extend(temp_here[m].astype(float).tolist())

    if not dt_vals:
        return pd.Series(dtype=float), pd.Series(dtype=float)
    return pd.Series(dt_vals, dtype=float), pd.Series(dt_temps, dtype=float)


def write_temp_bin_significance_all_devices(
    readings_df: pd.DataFrame,
    out_dir: Path,
    filter_tag: str,
    temp_bin_size_c: float = 5.0,
    dt_max_ms: Optional[float] = None,
    stats_alpha: float = 0.05,
    stats_min_samples: int = 200,
    stats_n_permutations: int = 2000,
) -> Optional[Path]:
    """
    Compute temp-bin Δt significance for all devices and write ONE combined CSV.

    This replaces the previous per-device stats CSVs.
    """
    if readings_df.empty or "temperature_c" not in readings_df.columns:
        LOGGER.info("Skipping temp-bin significance CSV (no temperature data).")
        return None

    out_dir.mkdir(parents=True, exist_ok=True)
    bin_size = float(temp_bin_size_c) if temp_bin_size_c and temp_bin_size_c > 0 else 5.0

    all_rows: list[dict] = []

    for device_uid, g in readings_df.groupby("device_uid", sort=True):
        dt_series, temp_series = collect_dt_and_temp_samples(g, dt_max_ms=dt_max_ms)
        if dt_series.empty or temp_series.empty:
            continue

        bin_low = (np.floor(temp_series / bin_size) * bin_size).astype(float)
        bins = sorted(bin_low.unique())
        if not bins:
            continue

        # Prepare per-bin samples
        samples_by_bin: dict[float, np.ndarray] = {}
        for b0 in bins:
            vals = dt_series[bin_low == b0].to_numpy(dtype=float)
            vals = vals[np.isfinite(vals)]
            if vals.size:
                samples_by_bin[float(b0)] = vals

        bins_sorted = sorted(samples_by_bin.keys())

        # Global test (optional SciPy)
        groups_for_global = [samples_by_bin[b] for b in bins_sorted if samples_by_bin[b].size >= stats_min_samples]
        global_p = float("nan")
        if _spstats is not None and len(groups_for_global) >= 2:  # pragma: no cover
            try:
                _, global_p = _spstats.kruskal(*groups_for_global)
            except Exception:
                global_p = float("nan")

        # Adjacent-bin permutation tests (median)
        pair_ps: list[float] = []
        pair_meta: list[tuple[float, float, int, int, float, float]] = []
        for i in range(len(bins_sorted) - 1):
            b0 = bins_sorted[i]
            b1 = bins_sorted[i + 1]
            a = samples_by_bin[b0]
            b = samples_by_bin[b1]
            if a.size < stats_min_samples or b.size < stats_min_samples:
                continue
            p = _permutation_test_median(
                a,
                b,
                n_perm=int(stats_n_permutations),
                seed=hash((device_uid, b0, b1)) & 0xFFFFFFFF,
            )
            pair_ps.append(p)
            pair_meta.append((b0, b1, int(a.size), int(b.size), float(np.median(a)), float(np.median(b))))

        if not pair_ps:
            continue

        q = _bh_fdr(np.asarray(pair_ps, dtype=float))
        for (b0, b1, na, nb, med_a, med_b), p, qq in zip(pair_meta, pair_ps, q):
            all_rows.append(
                {
                    "device_uid": device_uid,
                    "bin0_c": b0,
                    "bin1_c": b1,
                    "n0": na,
                    "n1": nb,
                    "median0_ms": med_a,
                    "median1_ms": med_b,
                    "delta_median_ms": med_a - med_b,
                    "p_perm_median": p,
                    "q_bh": float(qq),
                    "alpha": float(stats_alpha),
                    "significant": bool(np.isfinite(qq) and qq <= float(stats_alpha)),
                    "global_p_kruskal": global_p,
                    "temp_bin_size_c": float(bin_size),
                    "dt_max_ms": (float(dt_max_ms) if dt_max_ms is not None else np.nan),
                    "filter_tag": str(filter_tag),
                }
            )

    if not all_rows:
        LOGGER.info("No temp-bin significance rows computed (min samples too high or insufficient temperature coverage).")
        return None

    df_all = pd.DataFrame(all_rows)
    out_csv = out_dir / f"{_sanitize_filename_fragment(filter_tag)}_dt_temp_bin_significance_all_devices.csv"
    written = _write_csv_with_fallback(df_all, out_csv)
    if written != out_csv:
        LOGGER.warning("Combined stats CSV target locked; wrote instead: %s", written)

    n_sig = int(df_all["significant"].sum()) if "significant" in df_all.columns else 0
    LOGGER.info(
        "Wrote combined temp-bin significance CSV: %s (rows=%d, significant=%d, alpha=%.3f)",
        written,
        int(len(df_all)),
        n_sig,
        float(stats_alpha),
    )
    return written


def main() -> int:
    """
    Script entrypoint.

    Returns:
        Process exit code (0 for success).
    """
    cli = _parse_args()
    setup_logging(cli.log_level, cli.log_file)

    LOGGER.debug("CLI args: %s", cli)

    sessions, readings = parse_txt_files(cli.root, cli.pattern)
    readings = add_gateway_device_lag(readings)

    # Log timestamp gaps *within* each file per device (sessions ignore across-file gaps).
    log_intra_file_gaps(
        readings_df=readings,
        threshold_ms=cli.gap_threshold_ms,
        log_details=cli.log_gap_details,
    )
    
    analysis_readings = select_analysis_readings(readings, cli.filter_date)
    if not cli.filter_date or str(cli.filter_date).strip().lower() in {"all", "*"}:
        LOGGER.info("Analysis scope: all dates (%d readings)", len(analysis_readings))
        filter_tag = "all"
    else:
        LOGGER.info("Analysis scope: date=%s (%d readings)", cli.filter_date, len(analysis_readings))
        filter_tag = str(cli.filter_date).strip()

    # Export within-file timestamp gaps (one CSV per device serial; between-file gaps excluded).
    write_intra_file_gap_csvs(
        readings_df=analysis_readings,
        out_dir=Path(cli.gap_csv_dir),
        filter_tag=filter_tag,
        threshold_ms=float(cli.gap_threshold_ms),
    )

    # Always write ONE combined temp-bin significance CSV (headless-friendly).
    write_temp_bin_significance_all_devices(
        readings_df=analysis_readings,
        out_dir=Path(cli.plots_dir),
        filter_tag=filter_tag,
        temp_bin_size_c=float(cli.temp_bin_size_c),
        dt_max_ms=float(cli.gap_threshold_ms),
        stats_alpha=float(cli.temp_bin_test_alpha),
        stats_min_samples=int(cli.temp_bin_test_min_samples),
        stats_n_permutations=int(cli.temp_bin_test_permutations),
    )

    # Summary of parsed data
    LOGGER.info("=" * 80)
    LOGGER.info("Data Summary")
    LOGGER.info("=" * 80)
    LOGGER.info("Total sessions: %d", len(sessions))
    LOGGER.info("Total readings: %d", len(readings))
    if not readings.empty:
        LOGGER.info("Unique devices: %d", readings["device_uid"].nunique(dropna=True))
        LOGGER.info("Unique dosimeters (last-3-digit label): %d", readings["dosimeter_num"].nunique(dropna=True))
    else:
        LOGGER.info("Unique devices: 0")
        LOGGER.info("Unique dosimeters (last-3-digit label): 0")
    
    if not sessions.empty:
        LOGGER.info(
            "Session date range: %s to %s", sessions["started_at"].min(), sessions["ended_at"].max()
        )
        LOGGER.info("Sessions by device (global): %d", sessions["device_uid"].nunique(dropna=True))
    
    if not readings.empty:
        LOGGER.info("Readings by file:\n%s", readings.groupby("source_file").size().to_string())
        LOGGER.info("Readings by device:\n%s", readings.groupby("device_uid").size().to_string())
    
    # Display sample data
    if not sessions.empty:
        LOGGER.info("=" * 80)
        LOGGER.info("Sample Sessions (head):\n%s", sessions.head(10).to_string(index=False))
    
    if not readings.empty:
        sample_cols = ["id", "device_uid", "dosimeter_label", "captured_at", "charge_count"]
        available_cols = [c for c in sample_cols if c in readings.columns]
        LOGGER.info("=" * 80)
        LOGGER.info("Sample Readings (head):\n%s", readings[available_cols].head(10).to_string(index=False))
    
    # ============================================================================
    # Plots (ONLY):
    #   1) Time vs Index per device
    #   2) Histogram of Δt per device
    # ============================================================================
    if cli.no_plots:
        LOGGER.info("Plotting disabled (--no-plots).")
    else:
        make_device_plots(
            readings_df=analysis_readings,
            plots_dir=Path(cli.plots_dir),
            show=bool(cli.show_plots),
            # Exclude large Δt gaps (dropouts) from the candle using the same threshold
            # we use to *log* gaps. Between-file gaps are already excluded by construction.
            dt_max_ms=float(cli.gap_threshold_ms),
        )
        make_device_lag_plots(
            readings_df=analysis_readings,
            plots_dir=Path(cli.plots_dir),
            show=bool(cli.show_plots),
        )
        # Additional plots: Δt candle distributions broken down by temperature bins.
        make_device_temp_bin_candle_plots(
            readings_df=analysis_readings,
            plots_dir=Path(cli.plots_dir),
            temp_bin_size_c=float(cli.temp_bin_size_c),
            dt_max_ms=float(cli.gap_threshold_ms),
            stats_alpha=float(cli.temp_bin_test_alpha),
            stats_min_samples=int(cli.temp_bin_test_min_samples),
            stats_n_permutations=int(cli.temp_bin_test_permutations),
            show=bool(cli.show_plots),
        )
    
    # Analyze reporting frequency per device (Δt distribution summary)
    LOGGER.info("=" * 80)
    LOGGER.info("Reporting Frequency Analysis")
    LOGGER.info("=" * 80)
    
    freq_data = []

    if analysis_readings.empty:
        LOGGER.warning("No readings available for frequency analysis.")
    else:
        for device_uid, g in analysis_readings.groupby("device_uid", sort=True):
            g = g.sort_values("captured_at")
            # Use intra-file deltas only to avoid between-file gap outliers.
            deltas_ms_all = within_file_deltas_ms(g)
            # Exclude large dropouts/pauses (but they are still logged by `log_intra_file_gaps`).
            deltas_ms = deltas_ms_all[deltas_ms_all <= float(cli.gap_threshold_ms)]
            if deltas_ms.empty:
                continue
            avg_ms = float(deltas_ms.mean())
            std_ms = float(deltas_ms.std())
            min_ms = float(deltas_ms.min())
            max_ms = float(deltas_ms.max())
            freq_hz = (1000.0 / avg_ms) if avg_ms > 0 else 0.0
            LOGGER.info(
                "%s: avg=%.1fms (±%.1fms), range=[%.1f, %.1f]ms, ~%.2f Hz",
                device_uid,
                avg_ms,
                std_ms,
                min_ms,
                max_ms,
                freq_hz,
            )
            freq_data.append(
                {
                    "device_uid": device_uid,
                    "avg_ms": avg_ms,
                    "std_ms": std_ms,
                    "min_ms": min_ms,
                    "max_ms": max_ms,
                    "freq_hz": freq_hz,
                }
            )
    
    freq_df = pd.DataFrame(freq_data)
    if len(freq_df) == 0:
        LOGGER.warning("No frequency summary rows computed.")
    else:
        LOGGER.info(
            "Overall: Mean=%.2f Hz, Std=%.2f Hz",
            float(freq_df["freq_hz"].mean()),
            float(freq_df["freq_hz"].std()),
        )
    
    # ============================================================================
    # 4. Export Session Data to CSV
    # ============================================================================
    
    os.makedirs(cli.export_dir, exist_ok=True)
    if analysis_readings.empty:
        LOGGER.warning("No readings to export.")
    else:
        # Export one CSV per device for the analysis subset.
        tag = "all" if str(cli.filter_date).strip().lower() in {"all", "*", ""} else str(cli.filter_date).strip()
        for device_uid, g in analysis_readings.groupby("device_uid", sort=True):
            g = g.sort_values("captured_at").copy()
            if g.empty:
                continue

            # Add session-like bounds for the exported subset.
            g["session_started_at"] = pd.to_datetime(g["captured_at"], errors="coerce").min()
            g["session_ended_at"] = pd.to_datetime(g["captured_at"], errors="coerce").max()

            export_cols = [
                "id",
                "monitoring_session_id",
                "dosimeter_label",
                "device_uid",
                "charge_count",
                "captured_at",
                "log_time",
                "lag_seconds",
                "created_at",
                "source_file",
                "session_started_at",
                "session_ended_at",
            ]
            export_cols = [c for c in export_cols if c in g.columns]
            export_df = g[export_cols].copy()

            serial_digits = str(device_uid).split("-")[-1]
            out_path = cli.export_dir / f"{tag}_device_{_sanitize_filename_fragment(serial_digits)}.csv"
            written = _write_csv_with_fallback(export_df, out_path)
            if written != out_path:
                LOGGER.warning("Export target was locked; wrote instead: %s", written)
            LOGGER.info("Exported: %s (%d readings)", written, len(export_df))
    
    # Data parsing complete - sessions and readings DataFrames are ready for analysis
    LOGGER.info("=" * 80)
    LOGGER.info("Data parsing complete!")
    LOGGER.info("Total sessions: %d", len(sessions))
    LOGGER.info("Total readings: %d", len(readings))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
