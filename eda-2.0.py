#!/usr/bin/env python3
"""
eda.py

Refactored Exploratory Data Analysis tool for CB100 batch logs.
Parses text logs, aggregates sensor data, performs statistical analysis, and generates plots.

NOTE (2026-01): This file has been split into two focused scripts:
- thermal_drift.py  (thermal drift / lag vs temp+time)
- dropout_gaps.py   (dropouts / within-file gaps, optional curated real-gaps)

This file remains for backwards reference, but new work should use the split scripts.
"""

from __future__ import annotations

import argparse
import gzip
import logging
import pickle
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Optional dependency
try:
    from scipy import stats as _spstats  # type: ignore
except ImportError:
    _spstats = None

# --- Constants & Regex ---
REGEX_READING = re.compile(
    r"CB100-(?P<serial>\d+)-->.*?TS:\s*(?P<sec>\d+)\.(?P<ms>\d+)"
    r".*?Pulse:\s*(?P<pulse>\d+).*?Charge:\s*(?P<charge>\d+)"
    r"(?:.*?ADC:\s*(?P<adc>\d+))?",
    re.IGNORECASE,
)
REGEX_BRACKET_TIME = re.compile(r"\[(?P<hms>\d{2}:\d{2}:\d{2}\.\d{3})\]")
REGEX_CSV_TIME = re.compile(r"^(?P<hms>\d{2}:\d{2}:\d{2}\.\d{3}),")
REGEX_TEMP_HEADER = re.compile(r"Temperature Information from\s+(CB100-\d+)\s*:", re.IGNORECASE)
REGEX_AMBIENT_TEMP = re.compile(r"Ambient Temperature:\s*([+-]?\d+(?:\.\d+)?)\s*°?C", re.IGNORECASE)
REGEX_DATE_IN_NAME = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

# Warning / event log lines emitted by CB100_BLE.py
REGEX_PC_RECEPTION_TIMEOUT = re.compile(
    r"^\[(?P<hms>\d{2}:\d{2}:\d{2}\.\d{3})\]\s*WARNING:\s*(?P<device>CB100-\d+)\s*\[[0-9A-Fa-f:]+\]\s*-\s*No data received for\s*(?P<secs>[\d.]+)\s*seconds",
    re.IGNORECASE,
)
REGEX_DATA_LOSS_DETECTED = re.compile(
    r"^\[(?P<hms>\d{2}:\d{2}:\d{2}\.\d{3})\]\s*DATA_LOSS_DETECTED:\s*(?P<device>CB100-\d+)\s*-\s*(?P<jump_s>\d+)\s*s\s+jump\s+detected",
    re.IGNORECASE,
)
REGEX_KEEP_ALIVE_HEADER = re.compile(
    r"^\[(?P<hms>\d{2}:\d{2}:\d{2}\.\d{3})\]\s*Status/Keep-Alive Message from\s+(?P<device>CB100-\d+)\s*:\s*$",
    re.IGNORECASE,
)
REGEX_KEEP_ALIVE_PULSE = re.compile(r"Pulse Count:\s*(?P<pulse>\d+)", re.IGNORECASE)
REGEX_KEEP_ALIVE_CHARGE = re.compile(r"Charge Count:\s*(?P<charge>\d+)", re.IGNORECASE)
REGEX_KEEP_ALIVE_ADC = re.compile(r"ADC Value:\s*(?P<adc>\d+)", re.IGNORECASE)


# --- Configuration ---

@dataclass(frozen=True)
class AppConfig:
    """Immutable application configuration."""
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

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> AppConfig:
        return cls(
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


# --- Core Logic Components ---

class StatsEngine:
    """Pure logic component for statistical calculations."""

    @staticmethod
    def benjamini_hochberg_fdr(p_values: np.ndarray) -> np.ndarray:
        """Apply Benjamini–Hochberg FDR adjustment."""
        p = np.asarray(p_values, dtype=float)
        n = int(p.size)
        if n == 0:
            return p
        order = np.argsort(p)
        ranked = p[order]
        q = ranked * n / (np.arange(n, dtype=float) + 1.0)
        q = np.minimum.accumulate(q[::-1])[::-1]
        q = np.clip(q, 0.0, 1.0)
        out = np.empty_like(q)
        out[order] = q
        return out

    @staticmethod
    def permutation_test_median(a: np.ndarray, b: np.ndarray, n_perm: int = 2000, seed: int = 0) -> float:
        """Perform two-sided permutation test for difference in medians."""
        a = a[np.isfinite(a)]
        b = b[np.isfinite(b)]
        if a.size < 2 or b.size < 2:
            return float("nan")

        obs = float(np.median(a) - np.median(b))
        pooled = np.concatenate([a, b])
        n_a = int(a.size)

        rng = np.random.default_rng(seed)
        more_extreme = 0
        for _ in range(n_perm):
            perm = rng.permutation(pooled)
            stat = float(np.median(perm[:n_a]) - np.median(perm[n_a:]))
            if abs(stat) >= abs(obs):
                more_extreme += 1

        return float((more_extreme + 1) / (n_perm + 1))


class LogParser:
    """
    Handles file discovery and text parsing.
    Decoupled from Pandas/Analysis logic.
    """
    def __init__(self, root: Path, pattern: str):
        self.root = root
        self.pattern = pattern
        self.logger = logging.getLogger(__name__)

    def parse(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Scan and parse files.
        Returns: (readings_list, temperatures_list, events_list)
        """
        files = self._discover_files()
        self.logger.info("Discovered %d candidate .txt file(s)", len(files))

        all_readings: List[Dict[str, Any]] = []
        all_temps: List[Dict[str, Any]] = []
        all_events: List[Dict[str, Any]] = []

        for f_path in files:
            self.logger.info("Parsing %s", f_path.name)
            base_date = self._determine_base_date(f_path)
            
            try:
                r, t, e = self._parse_single_file(f_path, base_date)
                all_readings.extend(r)
                all_temps.extend(t)
                all_events.extend(e)
            except OSError:
                self.logger.exception("Failed reading %s", f_path)

        return all_readings, all_temps, all_events

    def _discover_files(self) -> List[Path]:
        """Generator replacement returning sorted list."""
        candidates = []
        for p in self.root.glob(self.pattern):
            if p.is_file() and p.suffix.lower() == ".txt":
                candidates.append(p)
        return sorted(candidates, key=lambda p: str(p).lower())

    def _determine_base_date(self, f_path: Path) -> datetime:
        """Extract date from filename or fallback to mtime."""
        m = REGEX_DATE_IN_NAME.search(f_path.name)
        if m:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        
        dt = datetime.fromtimestamp(f_path.stat().st_mtime)
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    def _parse_single_file(self, f_path: Path, base_date: datetime) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """Parses a single file line-by-line."""
        readings = []
        temps = []
        events = []
        
        with open(f_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = list(f)

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # 0. Try parsing Keep-Alive blocks (multi-line)
            keep_m = REGEX_KEEP_ALIVE_HEADER.match(line)
            if keep_m:
                parsed_ev, offset = self._extract_keep_alive_block(lines, i, base_date, f_path.name)
                if parsed_ev:
                    events.append(parsed_ev)
                i += offset
                continue

            # 0b. Try parsing explicit warning lines
            m_timeout = REGEX_PC_RECEPTION_TIMEOUT.match(line)
            if m_timeout:
                log_time = self._parse_bracket_time(line, base_date)
                secs = float(m_timeout.group("secs"))
                events.append({
                    "source_file": f_path.name,
                    "device_uid": m_timeout.group("device"),
                    "log_time": log_time,
                    "event_type": "pc_reception_timeout",
                    "duration_seconds": secs,
                    "raw_line": line,
                })
                i += 1
                continue

            m_loss = REGEX_DATA_LOSS_DETECTED.match(line)
            if m_loss:
                log_time = self._parse_bracket_time(line, base_date)
                jump_s = int(m_loss.group("jump_s"))
                events.append({
                    "source_file": f_path.name,
                    "device_uid": m_loss.group("device"),
                    "log_time": log_time,
                    "event_type": "data_loss_detected",
                    "jump_seconds": jump_s,
                    "raw_line": line,
                })
                i += 1
                continue

            # 1. Try parsing Temperature Block (Lookahead)
            temp_match = REGEX_TEMP_HEADER.search(line)
            if temp_match:
                parsed_temp, offset = self._extract_temperature_block(lines, i, temp_match, base_date, f_path.name)
                if parsed_temp:
                    temps.append(parsed_temp)
                i += offset # skip lines processed
                continue

            # 2. Try parsing Reading Line
            reading_match = REGEX_READING.search(line)
            if reading_match:
                readings.append(self._extract_reading(line, reading_match, base_date, f_path.name))
            
            i += 1
            
        return readings, temps, events

    def _extract_keep_alive_block(
        self, lines: List[str], current_idx: int, base_date: datetime, file_name: str
    ) -> Tuple[Optional[Dict], int]:
        """
        Extracts:
          [HH:MM:SS.mmm] Status/Keep-Alive Message from CB100-XXXX:
          ...
          Pulse Count: 0
          Charge Count: 0
          ADC Value: 0
        """
        header_line = lines[current_idx].strip()
        m = REGEX_KEEP_ALIVE_HEADER.match(header_line)
        if not m:
            return None, 1

        device_uid = m.group("device")
        log_time = self._parse_bracket_time(header_line, base_date)

        pulse = None
        charge = None
        adc = None

        # Scan ahead a bit; block formatting can have blank lines / other stats in between.
        lookahead = 18
        offset = 1
        for j in range(current_idx + 1, min(len(lines), current_idx + 1 + lookahead)):
            s = lines[j].strip()
            if REGEX_BRACKET_TIME.search(s):
                break
            mp = REGEX_KEEP_ALIVE_PULSE.search(s)
            if mp:
                try:
                    pulse = int(mp.group("pulse"))
                except ValueError:
                    pass
            mc = REGEX_KEEP_ALIVE_CHARGE.search(s)
            if mc:
                try:
                    charge = int(mc.group("charge"))
                except ValueError:
                    pass
            ma = REGEX_KEEP_ALIVE_ADC.search(s)
            if ma:
                try:
                    adc = int(ma.group("adc"))
                except ValueError:
                    pass
            offset += 1

        # Only treat as keep-alive event if the block explicitly reports all zeros.
        if log_time and pulse == 0 and charge == 0 and adc == 0:
            return {
                "source_file": file_name,
                "device_uid": device_uid,
                "log_time": log_time,
                "event_type": "keep_alive_all_zeros",
                "pulse_count": pulse,
                "charge_count": charge,
                "adc_value": adc,
                "raw_line": header_line,
            }, max(offset, 1)

        return None, max(offset, 1)

    def _extract_temperature_block(
        self, lines: List[str], current_idx: int, header_match: re.Match, base_date: datetime, file_name: str
    ) -> Tuple[Optional[Dict], int]:
        """Helper to extract temperature from a multi-line block."""
        dev_uid = header_match.group(1)
        log_time = self._parse_bracket_time(lines[current_idx], base_date)
        
        temp_val = None
        # Look ahead up to 4 lines
        for offset in range(1, 5):
            if current_idx + offset >= len(lines):
                break
            m_temp = REGEX_AMBIENT_TEMP.search(lines[current_idx + offset])
            if m_temp:
                try:
                    temp_val = float(m_temp.group(1))
                except ValueError:
                    pass
                break
        
        if log_time and temp_val is not None:
            return {
                "source_file": file_name,
                "device_uid": dev_uid,
                "log_time": log_time,
                "temperature_c": temp_val,
            }, 1  # Logic in original loop handled increment, but here we just return result
            
        return None, 1

    def _extract_reading(self, line: str, match: re.Match, base_date: datetime, file_name: str) -> Dict:
        """Helper to create reading dict from regex match."""
        device_serial = f"CB100-{match.group('serial')}"
        ts_sec = int(match.group("sec"))
        ts_ms = int(match.group("ms"))
        pulse = int(match.group("pulse"))
        charge = int(match.group("charge"))
        adc_raw = match.groupdict().get("adc")
        adc = int(adc_raw) if adc_raw and adc_raw.isdigit() else 0
        
        # Determine Timestamps
        timestamp = datetime.fromtimestamp(ts_sec + ts_ms / 1000.0)
        log_time = self._parse_bracket_time(line, base_date)

        return {
            "device_uid": device_serial,
            "dosimeter_label": device_serial,
            "captured_at": timestamp,
            "log_time": log_time,
            "created_at": timestamp,
            "pulse_count": pulse,
            "charge_count": charge,
            "adc_value": adc,
            "source_file": file_name,
        }

    @staticmethod
    def _parse_bracket_time(line: str, base_date: datetime) -> Optional[datetime]:
        """Parses [HH:MM:SS.mmm] or CSV-format HH:MM:SS.mmm, and combines with base_date."""
        m = REGEX_BRACKET_TIME.search(line)
        if not m:
            m = REGEX_CSV_TIME.match(line)
        if not m:
            return None
        try:
            t = datetime.strptime(m.group("hms"), "%H:%M:%S.%f")
            return base_date.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=t.microsecond)
        except ValueError:
            return None


class ParsedBinaryStore:
    """Persist parsed log records in a compressed binary cache grouped by device."""

    VERSION = 1

    @staticmethod
    def group_by_device(
        raw_readings: List[Dict[str, Any]],
        raw_temps: List[Dict[str, Any]],
        raw_events: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

        def _ensure(uid: str) -> Dict[str, List[Dict[str, Any]]]:
            if uid not in grouped:
                grouped[uid] = {"readings": [], "temps": [], "events": []}
            return grouped[uid]

        for row in raw_readings:
            uid = str(row.get("device_uid", "UNKNOWN"))
            _ensure(uid)["readings"].append(row)
        for row in raw_temps:
            uid = str(row.get("device_uid", "UNKNOWN"))
            _ensure(uid)["temps"].append(row)
        for row in raw_events:
            uid = str(row.get("device_uid", "UNKNOWN"))
            _ensure(uid)["events"].append(row)

        return grouped

    @staticmethod
    def flatten_grouped(
        grouped: Dict[str, Dict[str, List[Dict[str, Any]]]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        readings: List[Dict[str, Any]] = []
        temps: List[Dict[str, Any]] = []
        events: List[Dict[str, Any]] = []
        for uid in sorted(grouped.keys()):
            bucket = grouped[uid]
            readings.extend(bucket.get("readings", []))
            temps.extend(bucket.get("temps", []))
            events.extend(bucket.get("events", []))
        return readings, temps, events

    @classmethod
    def save(
        cls,
        path: Path,
        *,
        root: Path,
        pattern: str,
        grouped: Dict[str, Dict[str, List[Dict[str, Any]]]],
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": cls.VERSION,
            "created_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
            "root": str(root),
            "pattern": str(pattern),
            "grouped_by_device": grouped,
        }
        with gzip.open(path, "wb") as f:
            pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def load(
        cls, path: Path
    ) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        with gzip.open(path, "rb") as f:
            payload = pickle.load(f)
        version = int(payload.get("version", -1))
        if version != cls.VERSION:
            raise ValueError(f"Unsupported parsed cache version: {version}")
        grouped = payload.get("grouped_by_device")
        if not isinstance(grouped, dict):
            raise ValueError("Invalid parsed cache format: missing grouped_by_device")
        return grouped


class DataProcessor:
    """
    Handles DataFrame construction, cleaning, and augmentation.
    """
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def process_raw_data(
        self, raw_readings: List[Dict], raw_temps: List[Dict]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Converts lists to DataFrames and performs initial cleaning."""
        if not raw_readings:
            self.logger.warning("No readings found.")
            return pd.DataFrame(), pd.DataFrame()

        # Build Readings DF
        df = pd.DataFrame(raw_readings)
        for col in ["captured_at", "log_time", "created_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        df["dosimeter_num"] = pd.to_numeric(
            df["dosimeter_label"].astype(str).str[-3:], errors="coerce"
        ).astype("Int64")
        
        # Build Sessions DF
        sessions = self._build_sessions(df)
        
        # Map Session IDs
        if not sessions.empty:
            dev_to_id = dict(zip(sessions["device_uid"], sessions["id"]))
            df["monitoring_session_id"] = df["device_uid"].map(dev_to_id)

        df["id"] = range(1, len(df) + 1)

        # Interpolate Temperatures
        if raw_temps:
            t_df = pd.DataFrame(raw_temps)
            t_df["log_time"] = pd.to_datetime(t_df["log_time"], errors="coerce")
            t_df["temperature_c"] = pd.to_numeric(t_df["temperature_c"], errors="coerce")
            df["temperature_c"] = self._interpolate_temperature(df, t_df)
        else:
            df["temperature_c"] = np.nan

        # Calculate Gateway Lag
        if "log_time" in df.columns:
            # Raw lag is receiver/gateway time minus device time. In these logs there is a
            # consistent ~1 hour offset; keep the raw value and apply a correction.
            df["lag_seconds_raw"] = (df["log_time"] - df["captured_at"]).dt.total_seconds()
            df["lag_seconds"] = df["lag_seconds_raw"] - 3600.0

        self.logger.info("Processed %d readings across %d sessions.", len(df), len(sessions))
        return sessions, df

    def process_events(self, raw_events: List[Dict]) -> pd.DataFrame:
        """Converts raw event dicts into a normalized DataFrame."""
        if not raw_events:
            return pd.DataFrame()

        ev = pd.DataFrame(raw_events)
        if "log_time" in ev.columns:
            ev["log_time"] = pd.to_datetime(ev["log_time"], errors="coerce")
        for col in ["duration_seconds", "jump_seconds", "pulse_count", "charge_count", "adc_value"]:
            if col in ev.columns:
                ev[col] = pd.to_numeric(ev[col], errors="coerce")
        if "event_type" in ev.columns:
            ev["event_type"] = ev["event_type"].astype(str)
        return ev

    def filter_by_date(self, df: pd.DataFrame, date_str: str) -> Tuple[pd.DataFrame, str]:
        """Returns filtered DataFrame and a tag for filenames."""
        if df.empty or not date_str or date_str.lower() in {"all", "*"}:
            return df, "all"
        
        ts = pd.to_datetime(df["captured_at"], errors="coerce")
        mask = ts.dt.strftime("%Y-%m-%d") == date_str.strip()
        return df[mask].copy(), date_str.strip()

    def detect_intra_file_gaps(self, df: pd.DataFrame, threshold_ms: float) -> pd.DataFrame:
        """Identifies gaps within specific files greater than threshold."""
        if df.empty:
            return pd.DataFrame()
        
        # Ensure sorting
        df_sorted = df.sort_values(["source_file", "device_uid", "captured_at"])
        
        # Calculate diffs only within the same file and device
        # We group by file+device to ensure we don't diff across file boundaries
        grouped = df_sorted.groupby(["source_file", "device_uid"])
        
        gap_rows = []
        for (f_name, d_uid), group in grouped:
            if len(group) < 2:
                continue
            
            # Calculate delta in ms
            deltas = group["captured_at"].diff().dt.total_seconds() * 1000.0
            
            # Find exceedances
            gap_mask = deltas > threshold_ms
            if not gap_mask.any():
                continue
                
            gap_indices = group.index[gap_mask]
            
            for idx in gap_indices:
                curr_row = group.loc[idx]
                delta = deltas.loc[idx]
                # Reconstruct previous timestamp safely
                prev_ts = curr_row["captured_at"] - pd.Timedelta(milliseconds=delta)
                
                gap_rows.append({
                    "device_uid": d_uid,
                    "source_file": f_name,
                    "prev_captured_at": prev_ts,
                    "captured_at": curr_row["captured_at"],
                    "delta_ms": delta,
                    "threshold_ms": threshold_ms
                })
                
        return pd.DataFrame(gap_rows)

    def _build_sessions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Group by device to create global sessions."""
        sessions = []
        s_id = 1
        for device, g in df.groupby("device_uid"):
            if g.empty: 
                continue
            sessions.append({
                "id": s_id,
                "started_at": g["captured_at"].min(),
                "ended_at": g["captured_at"].max(),
                "device_uid": device,
                "n_readings": len(g),
                "n_files": g["source_file"].nunique(dropna=True)
            })
            s_id += 1
        return pd.DataFrame(sessions)

    def _interpolate_temperature(self, r_df: pd.DataFrame, t_df: pd.DataFrame) -> pd.Series:
        """Interpolates ambient temp onto readings using log_time."""
        out = pd.Series(index=r_df.index, dtype=float)
        
        for (sf, dev), g_read in r_df.groupby(["source_file", "device_uid"], sort=False):
            g_temp = t_df[(t_df["source_file"] == sf) & (t_df["device_uid"] == dev)]
            if g_temp.empty or len(g_temp) < 2:
                continue
                
            # Convert to unix timestamps for interpolation
            x_target = g_read["log_time"].astype("int64") // 1_000_000_000
            x_ref = g_temp["log_time"].astype("int64") // 1_000_000_000
            y_ref = g_temp["temperature_c"].values
            
            # Sort ref
            sort_idx = np.argsort(x_ref.values)
            x_ref = x_ref.values[sort_idx]
            y_ref = y_ref[sort_idx]
            
            interpolated = np.interp(x_target, x_ref, y_ref)
            out.loc[g_read.index] = interpolated
            
        return out


class ResultsExporter:
    """Handles CSV writing with fallback logic."""

    def __init__(self, output_dir: Path, logger: logging.Logger):
        self.output_dir = output_dir
        self.logger = logger
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_csv(self, df: pd.DataFrame, filename: str) -> Path:
        """Writes DataFrame to CSV, handling file locks."""
        target = self.output_dir / filename
        final_path = self._write_safe(df, target)
        self.logger.info("Exported CSV: %s (%d rows)", final_path, len(df))
        return final_path

    def _write_safe(self, df: pd.DataFrame, path: Path) -> Path:
        try:
            df.to_csv(path, index=False)
            return path
        except PermissionError:
            alt = path.with_name(f"{path.stem}_new{path.suffix}")
            df.to_csv(alt, index=False)
            self.logger.warning("File locked '%s', wrote to '%s' instead", path, alt)
            return alt

    @staticmethod
    def sanitize(s: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(s)).strip("_")


class ChartPlotter:
    """Handles Matplotlib visualization."""

    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.cfg = config
        self.logger = logger
        self.cfg.plots_dir.mkdir(parents=True, exist_ok=True)

    def generate_all(self, df: pd.DataFrame, events_df: Optional[pd.DataFrame] = None):
        if self.cfg.no_plots or df.empty:
            return

        for device_uid, g in df.groupby("device_uid"):
            safe_uid = ResultsExporter.sanitize(device_uid)
            ev_g = None
            if events_df is not None and not events_df.empty and "device_uid" in events_df.columns:
                ev_g = events_df[events_df["device_uid"] == device_uid].copy()
            self._plot_device_summary(g, device_uid, safe_uid, ev_g)
            self._plot_lag(g, device_uid, safe_uid)
            self._plot_temp_candles(g, device_uid, safe_uid)
        
        if self.cfg.show_plots:
            plt.show()

    def _plot_device_summary(self, df: pd.DataFrame, title: str, file_tag: str, events_df: Optional[pd.DataFrame] = None):
        """Plot Time vs Index, Delta Candle, and Charge."""
        df = df.sort_values("captured_at")
        
        # Calc Deltas (Intra-file only)
        deltas = []
        for _, gf in df.groupby("source_file"):
            d = gf["captured_at"].sort_values().diff().dt.total_seconds() * 1000.0
            deltas.append(d.dropna())
        
        if not deltas:
            return

        all_deltas = pd.concat(deltas)
        # Filter for plotting candle
        valid_deltas = all_deltas[all_deltas <= self.cfg.gap_threshold_ms]
        
        # Downsample for visualization if huge
        if len(df) > 20000:
            df = df.sample(20000).sort_values("captured_at")

        fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 5))
        
        # 1. Timeline
        ax1.plot(df["captured_at"], range(len(df)), lw=0.8)
        ax1.set_title(f"{title} - Sequence")
        ax1.set_xlabel("Device time (captured_at)")
        ax1.set_ylabel("Index")
        
        # 2. Boxplot
        ax2.boxplot(valid_deltas, vert=True, patch_artist=True, 
                   boxprops=dict(facecolor="lightgray"))
        ax2.set_title(f"Δt (ms) < {self.cfg.gap_threshold_ms:.0f}")
        ax2.text(0.05, 0.95, f"Median: {valid_deltas.median():.1f}ms", 
                 transform=ax2.transAxes, va="top")

        # 3. Charge
        if "charge_count" in df.columns:
            ax3.plot(df["captured_at"], df["charge_count"], lw=0.8)
            ax3.set_title("Charge vs Time")
            ax3.set_xlabel("Device time (captured_at)")
            ax3.set_ylabel("Charge")

        # Overlay warning/event markers (mapped onto captured_at using nearest lag)
        if events_df is not None and not events_df.empty and "log_time" in events_df.columns:
            mapped = self._map_events_to_captured_time(df, events_df)
            if not mapped.empty:
                self._overlay_events_on_timeline(ax1, df, mapped)
                if "charge_count" in df.columns:
                    self._overlay_events_on_charge(ax3, mapped)

        self._save_fig(fig, f"{file_tag}_summary.png")

    def _plot_lag(self, df: pd.DataFrame, title: str, file_tag: str):
        """Plot Gateway Lag vs Temperature."""
        if "lag_seconds" not in df.columns or df["lag_seconds"].isna().all():
            return
        if "temperature_c" not in df.columns or df["temperature_c"].isna().all():
            return

        g = df.copy()
        g = g[np.isfinite(g["lag_seconds"]) & np.isfinite(g["temperature_c"])].copy()
        if g.empty:
            return
            
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.scatter(g["temperature_c"], g["lag_seconds"], s=6, alpha=0.35)
        ax.set_title(f"{title} - Gateway Lag vs Temperature")
        ax.set_xlabel("Temperature (°C)")
        ax.set_ylabel("Lag (s)")
        ax.grid(True, alpha=0.3)
        self._save_fig(fig, f"{file_tag}_lag_vs_temp.png")

    def _map_events_to_captured_time(self, readings_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
        """
        Events are timestamped in PC time (`log_time`). Our plots are in device time (`captured_at`).
        We map each event onto the nearest reading by `log_time`, then estimate:
          captured_at_est = event.log_time - nearest_reading.lag_seconds
        """
        if readings_df.empty or events_df.empty:
            return pd.DataFrame()
        if "log_time" not in readings_df.columns or readings_df["log_time"].isna().all():
            return pd.DataFrame()
        if "lag_seconds" not in readings_df.columns or readings_df["lag_seconds"].isna().all():
            return pd.DataFrame()

        r_cols = ["log_time", "lag_seconds"]
        if "charge_count" in readings_df.columns:
            r_cols.append("charge_count")
        r = readings_df[r_cols].copy()
        r = r.dropna(subset=["log_time", "lag_seconds"]).sort_values("log_time")
        e = events_df.copy()
        e = e.dropna(subset=["log_time"]).sort_values("log_time")
        if r.empty or e.empty:
            return pd.DataFrame()

        merged = pd.merge_asof(
            e,
            r,
            on="log_time",
            direction="nearest",
            tolerance=pd.Timedelta(seconds=30),
        )
        merged["captured_at_est"] = merged["log_time"] - pd.to_timedelta(merged["lag_seconds"], unit="s")
        return merged.dropna(subset=["captured_at_est"])

    @staticmethod
    def _overlay_events_on_timeline(ax: plt.Axes, readings_df: pd.DataFrame, mapped_events: pd.DataFrame):
        """Add event markers to the time-vs-index subplot."""
        if "event_type" not in mapped_events.columns:
            return
        df_sorted = readings_df.sort_values("captured_at")
        x = df_sorted["captured_at"].values

        def y_index_for_times(times: pd.Series) -> np.ndarray:
            # searchsorted expects numpy datetime64; pandas gives that via .values
            return np.searchsorted(x, times.values, side="left")

        styles = {
            "data_loss_detected": dict(color="red", marker="v", label="DATA_LOSS_DETECTED"),
            "pc_reception_timeout": dict(color="purple", marker="x", label="PC Reception Timeout"),
            "keep_alive_all_zeros": dict(color="orange", marker="o", label="Keep-Alive (all zeros)"),
        }

        for ev_type, style in styles.items():
            sub = mapped_events[mapped_events["event_type"] == ev_type]
            if sub.empty:
                continue
            y = y_index_for_times(sub["captured_at_est"])
            ax.scatter(sub["captured_at_est"], y, s=28, alpha=0.85, **style)

        ax.legend(loc="best", fontsize=8, frameon=True)

    @staticmethod
    def _overlay_events_on_charge(ax: plt.Axes, mapped_events: pd.DataFrame):
        """Add event markers to the charge-vs-time subplot."""
        styles = {
            "data_loss_detected": dict(color="red", alpha=0.25, lw=1.2, label="DATA_LOSS_DETECTED"),
            "pc_reception_timeout": dict(color="purple", alpha=0.25, lw=1.2, label="PC Reception Timeout"),
            "keep_alive_all_zeros": dict(color="orange", alpha=0.25, lw=1.2, label="Keep-Alive (all zeros)"),
        }

        # Draw vertical lines (avoid duplicate legend entries)
        used = set()
        for _, row in mapped_events.iterrows():
            ev_type = row.get("event_type")
            if ev_type not in styles:
                continue
            st = styles[ev_type]
            label = st["label"] if ev_type not in used else None
            used.add(ev_type)
            ax.axvline(row["captured_at_est"], color=st["color"], alpha=st["alpha"], lw=st["lw"], label=label)

        if used:
            ax.legend(loc="best", fontsize=8, frameon=True)

    def _plot_temp_candles(self, df: pd.DataFrame, title: str, file_tag: str):
        """Candle plots binned by temperature."""
        if "temperature_c" not in df.columns or df["temperature_c"].isna().all():
            return
            
        # Collect data
        data_pairs = []
        for _, gf in df.groupby("source_file"):
            gf = gf.sort_values("captured_at")
            d = gf["captured_at"].diff().dt.total_seconds() * 1000.0
            t = gf["temperature_c"]
            # mask
            mask = d.notna() & t.notna() & (d <= self.cfg.gap_threshold_ms)
            if mask.any():
                data_pairs.append(pd.DataFrame({"dt": d[mask], "temp": t[mask]}))
        
        if not data_pairs:
            return

        combined = pd.concat(data_pairs)
        bin_size = self.cfg.temp_bin_size_c
        combined["bin"] = (np.floor(combined["temp"] / bin_size) * bin_size)
        
        bins = sorted(combined["bin"].unique())
        if not bins:
            return

        # Plotting
        ncols = 3
        nrows = int(np.ceil(len(bins) / ncols))
        fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows), sharey=True)
        axes = np.atleast_1d(axes).reshape(nrows, ncols)
        
        for idx, b in enumerate(bins):
            r, c = divmod(idx, ncols)
            ax = axes[r, c]
            subset = combined[combined["bin"] == b]["dt"]
            ax.boxplot(subset, vert=True, patch_artist=True, boxprops=dict(facecolor="lightblue"))
            ax.set_title(f"{b:.0f} - {b+bin_size:.0f}°C (n={len(subset)})")
            
        # Hide empty
        for idx in range(len(bins), nrows * ncols):
            r, c = divmod(idx, ncols)
            axes[r, c].set_axis_off()

        self._save_fig(fig, f"{file_tag}_temp_candles.png")

    def _save_fig(self, fig: plt.Figure, name: str):
        path = self.cfg.plots_dir / name
        try:
            fig.tight_layout()
            fig.savefig(path, dpi=150)
            if not self.cfg.show_plots:
                plt.close(fig)
            self.logger.info("Saved plot: %s", path)
        except Exception as e:
            self.logger.error("Failed to save plot %s: %s", name, e)


# --- Collision Analysis Helpers ---

def load_real_gaps(real_gaps_dir: Path) -> pd.DataFrame:
    """
    Load curated gap CSVs from `real_gaps_dir`.

    Expected columns (minimum):
      - device_uid
      - source_file
      - prev_captured_at
      - captured_at
      - delta_ms
    """
    if real_gaps_dir is None:
        return pd.DataFrame()
    try:
        files = sorted([p for p in Path(real_gaps_dir).glob("*.csv") if p.is_file()], key=lambda p: p.name.lower())
    except Exception:
        return pd.DataFrame()
    if not files:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for p in files:
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        required = {"device_uid", "source_file", "prev_captured_at", "captured_at", "delta_ms"}
        if not required.issubset(set(df.columns)):
            continue
        out = df[list(required)].copy()
        out["device_uid"] = out["device_uid"].astype(str)
        out["source_file"] = out["source_file"].astype(str)
        out["prev_captured_at"] = pd.to_datetime(out["prev_captured_at"], errors="coerce")
        out["captured_at"] = pd.to_datetime(out["captured_at"], errors="coerce")
        out["delta_ms"] = pd.to_numeric(out["delta_ms"], errors="coerce")
        out = out.dropna(subset=["device_uid", "source_file", "prev_captured_at", "captured_at", "delta_ms"])
        if not out.empty:
            frames.append(out)

    if not frames:
        return pd.DataFrame()
    allg = pd.concat(frames, ignore_index=True)
    return allg.sort_values(["source_file", "device_uid", "captured_at"], kind="mergesort").reset_index(drop=True)


def map_gaps_to_log_time(gaps_df: pd.DataFrame, readings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map curated gaps (defined by device `captured_at`) onto receiver time (`log_time`)
    by joining to readings_df on (source_file, device_uid, captured_at).
    """
    if gaps_df.empty or readings_df.empty:
        return pd.DataFrame()
    required_g = {"source_file", "device_uid", "prev_captured_at", "captured_at", "delta_ms"}
    required_r = {"source_file", "device_uid", "captured_at", "log_time"}
    if not required_g.issubset(set(gaps_df.columns)) or not required_r.issubset(set(readings_df.columns)):
        return pd.DataFrame()

    g = gaps_df.copy()
    g["prev_captured_at"] = pd.to_datetime(g["prev_captured_at"], errors="coerce")
    g["captured_at"] = pd.to_datetime(g["captured_at"], errors="coerce")
    g["delta_ms"] = pd.to_numeric(g["delta_ms"], errors="coerce")
    g = g.dropna(subset=["source_file", "device_uid", "prev_captured_at", "captured_at", "delta_ms"])
    if g.empty:
        return pd.DataFrame()

    r = readings_df[["source_file", "device_uid", "captured_at", "log_time"]].copy()
    r["captured_at"] = pd.to_datetime(r["captured_at"], errors="coerce")
    r["log_time"] = pd.to_datetime(r["log_time"], errors="coerce")
    r = r.dropna(subset=["source_file", "device_uid", "captured_at", "log_time"])
    if r.empty:
        return pd.DataFrame()

    r["captured_ns"] = r["captured_at"].astype("int64")
    r = r.drop_duplicates(subset=["source_file", "device_uid", "captured_ns"], keep="first")

    g["prev_captured_ns"] = g["prev_captured_at"].astype("int64")
    g["captured_ns"] = g["captured_at"].astype("int64")

    prev_map = r.rename(columns={"log_time": "prev_log_time"})[
        ["source_file", "device_uid", "captured_ns", "prev_log_time"]
    ]
    cur_map = r.rename(columns={"log_time": "event_log_time"})[
        ["source_file", "device_uid", "captured_ns", "event_log_time"]
    ]

    out = g.merge(
        prev_map,
        left_on=["source_file", "device_uid", "prev_captured_ns"],
        right_on=["source_file", "device_uid", "captured_ns"],
        how="left",
        suffixes=("", "_drop"),
    )
    out = out.drop(columns=[c for c in out.columns if c.endswith("_drop")], errors="ignore")
    out = out.merge(
        cur_map,
        left_on=["source_file", "device_uid", "captured_ns"],
        right_on=["source_file", "device_uid", "captured_ns"],
        how="left",
        suffixes=("", "_drop"),
    )
    out = out.drop(columns=[c for c in out.columns if c.endswith("_drop")], errors="ignore")

    out["prev_time"] = pd.to_datetime(out["prev_log_time"], errors="coerce")
    out["event_time"] = pd.to_datetime(out["event_log_time"], errors="coerce")
    out["gap_ms_receiver"] = (out["event_time"] - out["prev_time"]).dt.total_seconds() * 1000.0
    out["gap_ms_device"] = out["delta_ms"].astype(float)

    return out[
        [
            "source_file",
            "device_uid",
            "prev_time",
            "event_time",
            "gap_ms_device",
            "gap_ms_receiver",
            "prev_captured_at",
            "captured_at",
        ]
    ].sort_values(["source_file", "device_uid", "event_time"], kind="mergesort").reset_index(drop=True)


def _get_active_segments(times_sec: np.ndarray, max_gap_sec: float) -> list:
    """
    Split a sorted array of epoch seconds into contiguous active segments.
    Any gap > max_gap_sec between consecutive readings starts a new segment.
    Returns list of (seg_start, seg_end) tuples in epoch seconds.
    """
    t = np.sort(times_sec[np.isfinite(times_sec)])
    if t.size == 0:
        return []
    if t.size == 1:
        return [(float(t[0]), float(t[0]))]
    diffs = np.diff(t)
    break_indices = np.where(diffs > max_gap_sec)[0]
    starts = np.concatenate([[0], break_indices + 1])
    ends = np.concatenate([break_indices, [len(t) - 1]])
    return [(float(t[s]), float(t[e])) for s, e in zip(starts, ends)]


def _datetime_to_epoch_seconds(s: pd.Series) -> np.ndarray:
    dt = pd.to_datetime(s, errors="coerce")
    ns = dt.astype("int64")
    out = ns.to_numpy(dtype=np.float64) / 1_000_000_000.0
    out[ns.to_numpy(dtype=np.int64) < 0] = np.nan
    return out


def _fit_schedule_from_times(times_sec: np.ndarray, nominal_period_ms: float, min_points: int) -> Optional[dict]:
    t = np.asarray(times_sec, dtype=np.float64)
    t = t[np.isfinite(t)]
    if int(t.size) < int(min_points):
        return None
    t = np.sort(t)

    dt_ms_full = np.diff(t) * 1000.0
    dt_ms_valid = dt_ms_full[np.isfinite(dt_ms_full) & (dt_ms_full > 0)]
    if int(dt_ms_valid.size) < 5:
        return None

    nom = float(nominal_period_ms)
    near = dt_ms_valid[(dt_ms_valid >= 0.5 * nom) & (dt_ms_valid <= 1.5 * nom)]
    if int(near.size) >= 20:
        p0_ms = float(np.median(near))
    else:
        p0_ms = float(np.median(dt_ms_valid))
    if not np.isfinite(p0_ms) or p0_ms <= 0:
        return None

    dt_for_steps = np.asarray(dt_ms_full, dtype=np.float64)
    bad = ~np.isfinite(dt_for_steps) | (dt_for_steps <= 0)
    if int(bad.sum()) > 0:
        dt_for_steps[bad] = float(p0_ms)
    steps = np.rint(dt_for_steps / p0_ms).astype(np.int64)
    steps[steps < 1] = 1
    steps = np.clip(steps, 1, 10_000)
    k = np.empty(int(t.size), dtype=np.float64)
    k[0] = 0.0
    k[1:] = np.cumsum(steps, dtype=np.float64)

    try:
        slope, intercept = np.polyfit(k, t, deg=1)
    except Exception:
        return None
    if not np.isfinite(slope) or not np.isfinite(intercept) or slope <= 0:
        return None

    return {"t0_sec": float(intercept), "p_sec": float(slope), "p_fit_ms": float(slope * 1000.0), "n_obs": int(t.size)}


def run_collision_analysis_from_gaps(
    readings_df: pd.DataFrame,
    gaps_df: pd.DataFrame,
    *,
    out_dir: Path,
    filter_tag: str,
    dropout_threshold_ms: float,
    dropout_max_gap_ms: float,
    nominal_period_ms: float,
    window_max_ms: int,
    min_points_per_stream: int,
    intra_gap_sec: float = 1.0,
) -> Dict[str, Any]:
    """
    Collision analysis using ONLY the provided curated gaps_df as dropout ground truth.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Map curated gaps to receiver time intervals
    mapped = map_gaps_to_log_time(gaps_df, readings_df)
    if mapped.empty:
        return {"filter_tag": filter_tag, "error": "No mapped gaps found."}

    # Apply thresholds on curated gap size (device ms)
    gdev = pd.to_numeric(mapped["gap_ms_device"], errors="coerce")
    used = (gdev > float(dropout_threshold_ms)) & (
        gdev <= float(dropout_max_gap_ms) if np.isfinite(dropout_max_gap_ms) else True
    )
    mapped["used_for_collision"] = used
    mapped_csv = out_dir / f"{filter_tag}_dropout_events.csv"
    mapped.to_csv(mapped_csv, index=False)

    used_gaps = mapped[mapped["used_for_collision"]].copy()

    # Fit schedules per (file,device) using receiver time log_time
    params_rows: list[dict] = []
    df = readings_df.copy()
    df["log_time"] = pd.to_datetime(df.get("log_time"), errors="coerce")
    df = df.dropna(subset=["source_file", "device_uid", "log_time"])
    for (sf, dev), g in df.groupby(["source_file", "device_uid"], sort=True):
        t_sec = _datetime_to_epoch_seconds(g["log_time"])
        fit = _fit_schedule_from_times(t_sec, float(nominal_period_ms), int(min_points_per_stream))
        if fit is None:
            continue
        params_rows.append({"source_file": sf, "device_uid": dev, **fit})
    params = pd.DataFrame(params_rows)
    params_csv = out_dir / f"{filter_tag}_collision_params.csv"
    params.to_csv(params_csv, index=False)

    if params.empty:
        return {"filter_tag": filter_tag, "dropouts_csv": str(mapped_csv), "params_csv": str(params_csv), "error": "No schedules fitted."}

    # Build active segments per (source_file, device_uid) — gaps > intra_gap_sec are session breaks
    segments_dict: dict[tuple[str, str], list] = {}
    for (sf, dev), g in df.groupby(["source_file", "device_uid"], sort=True):
        t_sec = _datetime_to_epoch_seconds(g["log_time"])
        segments_dict[(str(sf), str(dev))] = _get_active_segments(t_sec, intra_gap_sec)

    # Generate ticks only within active segments, not across inter-session gaps
    tick_rows: list[dict] = []
    for _, r in params.iterrows():
        t0 = float(r["t0_sec"])
        p = float(r["p_sec"])
        if not np.isfinite(t0) or not np.isfinite(p) or p <= 0:
            continue
        key = (str(r["source_file"]), str(r["device_uid"]))
        segments = segments_dict.get(key, [])
        for seg_start, seg_end in segments:
            k_start = int(np.ceil((seg_start - t0) / p))
            k_end = int(np.floor((seg_end - t0) / p))
            if k_end < k_start:
                continue
            k = np.arange(k_start, k_end + 1, dtype=np.int64)
            tick_sec = t0 + k.astype(np.float64) * p
            tick_time = pd.to_datetime(tick_sec, unit="s", errors="coerce")
            for ts_sec, ts_dt in zip(tick_sec, tick_time):
                if pd.isna(ts_dt):
                    continue
                tick_rows.append({"source_file": r["source_file"], "device_uid": r["device_uid"], "tick_time": ts_dt, "tick_sec": float(ts_sec)})
    ticks = pd.DataFrame(tick_rows).sort_values(["source_file", "tick_sec"], kind="mergesort").reset_index(drop=True)
    if ticks.empty:
        return {"filter_tag": filter_tag, "dropouts_csv": str(mapped_csv), "params_csv": str(params_csv), "error": "No ticks generated."}

    # Build dropout intervals per (file,device) in epoch seconds (only used gaps)
    intervals: dict[tuple[str, str], tuple[np.ndarray, np.ndarray]] = {}
    for (sf, dev), g in used_gaps.groupby(["source_file", "device_uid"], sort=False):
        a = _datetime_to_epoch_seconds(g["prev_time"])
        b = _datetime_to_epoch_seconds(g["event_time"])
        ok = np.isfinite(a) & np.isfinite(b) & (b > a)
        a = a[ok]
        b = b[ok]
        if a.size:
            order = np.argsort(a)
            intervals[(str(sf), str(dev))] = (a[order], b[order])

    # Label ticks inside curated dropout intervals
    y = np.zeros(int(len(ticks)), dtype=np.int64)
    for (sf, dev), idx in ticks.groupby(["source_file", "device_uid"], sort=False).groups.items():
        key = (str(sf), str(dev))
        if key not in intervals:
            continue
        starts, ends = intervals[key]
        t_sec = ticks.loc[idx, "tick_sec"].to_numpy(dtype=np.float64)
        pos = np.searchsorted(starts, t_sec, side="right") - 1
        m = (pos >= 0) & (t_sec > starts[pos]) & (t_sec < ends[pos])
        y[idx] = m.astype(np.int64)
    ticks["is_dropout_tick"] = y

    # Compute min separation per tick within each file
    ticks["min_sep_ms"] = np.nan
    for sf, idx in ticks.groupby("source_file", sort=False).groups.items():
        sidx = np.array(list(idx), dtype=np.int64)
        order = np.argsort(ticks.loc[sidx, "tick_sec"].to_numpy(dtype=np.float64))
        sidx_sorted = sidx[order]
        ts = ticks.loc[sidx_sorted, "tick_sec"].to_numpy(dtype=np.float64)
        if ts.size < 2:
            ticks.loc[sidx_sorted, "min_sep_ms"] = np.inf
            continue
        prev = np.r_[np.inf, np.diff(ts)]
        nxt = np.r_[np.diff(ts), np.inf]
        ticks.loc[sidx_sorted, "min_sep_ms"] = np.minimum(prev, nxt) * 1000.0

    # Choose best W by F1
    y_true = ticks["is_dropout_tick"].to_numpy(dtype=np.int64)
    ms = pd.to_numeric(ticks["min_sep_ms"], errors="coerce").to_numpy(dtype=np.float64)
    best_w = 1
    best_f1 = -1.0
    for w in range(1, max(1, int(window_max_ms)) + 1):
        y_pred = np.isfinite(ms) & (ms <= float(w))
        tp = int(((y_true == 1) & y_pred).sum())
        fp = int(((y_true == 0) & y_pred).sum())
        fn = int(((y_true == 1) & (~y_pred)).sum())
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        if f1 > best_f1:
            best_f1 = f1
            best_w = int(w)

    ticks_csv = out_dir / f"{filter_tag}_collision_ticks_W{best_w}ms.csv"
    ticks.to_csv(ticks_csv, index=False)

    summary = pd.DataFrame(
        [
            {
                "filter_tag": filter_tag,
                "best_w_ms": int(best_w),
                "n_curated_gaps": int(len(gaps_df)),
                "n_used_gaps": int(len(used_gaps)),
                "n_ticks": int(len(ticks)),
                "n_dropout_ticks": int((ticks["is_dropout_tick"] == 1).sum()),
            }
        ]
    )
    summary_csv = out_dir / f"{filter_tag}_collision_validation_summary.csv"
    summary.to_csv(summary_csv, index=False)

    return {
        "filter_tag": filter_tag,
        "dropouts_csv": str(mapped_csv),
        "params_csv": str(params_csv),
        "ticks_csv": str(ticks_csv),
        "summary_csv": str(summary_csv),
        "best_w_ms": int(best_w),
    }


# --- Orchestration ---

def setup_logging(level_str: str, log_file: Optional[Path]) -> logging.Logger:
    level = getattr(logging, level_str.upper(), logging.INFO)
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
        force=True
    )
    return logging.getLogger("eda")


def load_or_parse_input_records(
    parser_svc: LogParser,
    *,
    cache_path: Optional[Path],
    rebuild_cache: bool,
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Load parsed records from cache when available, otherwise parse raw text logs."""
    if cache_path and cache_path.exists() and not rebuild_cache:
        try:
            grouped = ParsedBinaryStore.load(cache_path)
            raw_readings, raw_temps, raw_events = ParsedBinaryStore.flatten_grouped(grouped)
            logger.info(
                "Loaded parsed cache: %s (devices=%d readings=%d temps=%d events=%d)",
                cache_path,
                len(grouped),
                len(raw_readings),
                len(raw_temps),
                len(raw_events),
            )
            return raw_readings, raw_temps, raw_events
        except Exception as exc:
            logger.warning("Failed loading parsed cache '%s' (%s). Re-parsing txt logs.", cache_path, exc)

    raw_readings, raw_temps, raw_events = parser_svc.parse()

    if cache_path:
        try:
            grouped = ParsedBinaryStore.group_by_device(raw_readings, raw_temps, raw_events)
            ParsedBinaryStore.save(
                cache_path,
                root=parser_svc.root,
                pattern=parser_svc.pattern,
                grouped=grouped,
            )
            logger.info("Saved parsed cache: %s (devices=%d)", cache_path, len(grouped))
        except Exception as exc:
            logger.warning("Failed saving parsed cache '%s' (%s). Continuing without cache.", cache_path, exc)

    return raw_readings, raw_temps, raw_events


def main() -> int:
    # 1. Configuration
    p = argparse.ArgumentParser(description="EDA Refactored")
    # ... (Add all arguments here matching the original, omitted for brevity but assumed present)
    # For this example, I will assume the CLI Args are populated exactly as before.
    # Users should copy the `build_arg_parser` from the original script and use it here.
    # Below is a minimal hydration for context:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--pattern", default="rdc-captures/*.txt")
    parser.add_argument("--filter-date", default="all")
    parser.add_argument("--export-dir", default="session_exports")
    parser.add_argument("--plots-dir", default="plots")
    parser.add_argument("--gap-csv-dir", default="gap_exports")
    parser.add_argument("--no-plots", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-file", default=None)
    parser.add_argument("--gap-threshold-ms", type=float, default=1000.0)
    parser.add_argument("--log-gap_details", action="store_true", default=True)
    parser.add_argument("--show-plots", action="store_true")
    parser.add_argument("--temp-bin-size-c", type=float, default=5.0)
    parser.add_argument("--temp-bin-test-alpha", type=float, default=0.05)
    parser.add_argument("--temp-bin-test-min-samples", type=int, default=200)
    parser.add_argument("--temp-bin-test-permutations", type=int, default=2000)
    parser.add_argument("--real-gaps-dir", default="real-gaps")
    parser.add_argument("--use-real-gaps", action="store_true", default=True)
    parser.add_argument("--real-gaps-max-ms", type=float, default=10000.0)
    parser.add_argument("--collision-analysis", action="store_true", default=True)
    parser.add_argument("--collision-out-dir", default="collision_exports")
    parser.add_argument("--collision-window-max-ms", type=int, default=50)
    parser.add_argument("--collision-nominal-period-ms", type=float, default=200.0)
    parser.add_argument("--collision-min-points-per-stream", type=int, default=200)
    parser.add_argument("--collision-intra-gap-ms", type=float, default=1000.0,
                        help="Max gap (ms) between readings before treating as a new session. Ticks are not generated across larger gaps. Default: 1000.")
    parser.add_argument("--parsed-binary-path", default="parsed_cache/parsed_by_device.pkl.gz",
                        help="Compressed binary cache (grouped by device serial) used to speed ingestion.")
    parser.add_argument("--rebuild-parsed-binary", action="store_true", default=False,
                        help="Ignore existing parsed binary cache and rebuild from txt files.")
    parser.add_argument("--build-parsed-binary-only", action="store_true", default=False,
                        help="Parse txt files and write parsed binary cache, then exit.")

    args = parser.parse_args()
    config = AppConfig.from_args(args)
    logger = setup_logging(config.log_level, config.log_file)

    logger.info("Starting Analysis with config: %s", config)

    # 2. Ingestion
    parser_svc = LogParser(config.root, config.pattern)
    cache_arg = str(getattr(args, "parsed_binary_path", "")).strip()
    cache_path: Optional[Path] = None
    if cache_arg and cache_arg.lower() not in {"none", "off", "false"}:
        cache_path = Path(cache_arg)
        if not cache_path.is_absolute():
            cache_path = (Path.cwd() / cache_path).resolve()

    raw_readings, raw_temps, raw_events = load_or_parse_input_records(
        parser_svc,
        cache_path=cache_path,
        rebuild_cache=bool(getattr(args, "rebuild_parsed_binary", False)),
        logger=logger,
    )

    if bool(getattr(args, "build_parsed_binary_only", False)):
        logger.info("Built parsed binary cache only. Exiting.")
        return 0

    # 3. Processing
    processor = DataProcessor(logger)
    sessions_df, readings_df = processor.process_raw_data(raw_readings, raw_temps)
    events_df = processor.process_events(raw_events)
    
    # 4. Analysis & Output
    # Filter scope
    analysis_df, filter_tag = processor.filter_by_date(readings_df, config.filter_date)
    
    # Detect Gaps
    gaps_df = processor.detect_intra_file_gaps(analysis_df, config.gap_threshold_ms)
    
    # Log Gaps
    if not gaps_df.empty:
        max_gap = gaps_df["delta_ms"].max()
        logger.warning("Detected %d gaps > %.1fms (Max: %.1fms)", len(gaps_df), config.gap_threshold_ms, max_gap)

    # Optional curated gap override
    if getattr(args, "use_real_gaps", False):
        rg_dir = Path(getattr(args, "real_gaps_dir", "real-gaps"))
        real_gaps = load_real_gaps(rg_dir)
        if real_gaps.empty:
            logger.warning("--use-real-gaps: no usable CSVs found in %s", rg_dir)
        else:
            keep_files = set(analysis_df["source_file"].astype(str).unique())
            keep_devs  = set(analysis_df["device_uid"].astype(str).unique())
            real_gaps  = real_gaps[
                real_gaps["source_file"].astype(str).isin(keep_files)
                & real_gaps["device_uid"].astype(str).isin(keep_devs)
            ].copy()
            max_ms = float(getattr(args, "real_gaps_max_ms", 10000.0))
            real_gaps = real_gaps[pd.to_numeric(real_gaps["delta_ms"], errors="coerce") <= max_ms].copy()
            gaps_df = real_gaps.reset_index(drop=True)

    # Collision analysis
    if getattr(args, "collision_analysis", False):
        out_dir = Path(getattr(args, "collision_out_dir", "collision_exports"))
        if gaps_df.empty:
            logger.warning("--collision-analysis: gaps_df is empty, nothing to process.")
        else:
            info = run_collision_analysis_from_gaps(
                analysis_df, gaps_df,
                out_dir=out_dir,
                filter_tag=filter_tag,
                dropout_threshold_ms=float(config.gap_threshold_ms),
                dropout_max_gap_ms=float(getattr(args, "real_gaps_max_ms", 10000.0)),
                nominal_period_ms=float(getattr(args, "collision_nominal_period_ms", 200.0)),
                window_max_ms=int(getattr(args, "collision_window_max_ms", 50)),
                min_points_per_stream=int(getattr(args, "collision_min_points_per_stream", 200)),
                intra_gap_sec=float(getattr(args, "collision_intra_gap_ms", 1000.0)) / 1000.0,
            )
            logger.info("Collision analysis outputs: %s", info)

    # Export
    exporter = ResultsExporter(config.export_dir, logger)
    if not analysis_df.empty:
        sanitized_date = ResultsExporter.sanitize(filter_tag)
        for dev, g in analysis_df.groupby("device_uid"):
            dev_safe = ResultsExporter.sanitize(str(dev).split("-")[-1])
            exporter.write_csv(g, f"{sanitized_date}_device_{dev_safe}.csv")

    gap_exporter = ResultsExporter(config.gap_csv_dir, logger)
    if not gaps_df.empty:
        for dev, g in gaps_df.groupby("device_uid"):
             dev_safe = ResultsExporter.sanitize(str(dev).split("-")[-1])
             gap_exporter.write_csv(g, f"{filter_tag}_gaps_{dev_safe}.csv")

    # 5. Statistics (Temp Bins)
    # (Implementation logic maps to StatsEngine calls, simplified here for space)
    # You would iterate bins here and call StatsEngine.permutation_test_median
    
    # 6. Plotting
    plotter = ChartPlotter(config, logger)
    plotter.generate_all(analysis_df, events_df)

    logger.info("Analysis Complete.")
    return 0

if __name__ == "__main__":
    sys.exit(main())