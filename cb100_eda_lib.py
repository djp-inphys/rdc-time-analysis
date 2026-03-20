#!/usr/bin/env python3
"""
cb100_eda_lib.py

Shared parsing + processing utilities used by:
- thermal_drift.py
- dropout_gaps.py

This is extracted/trimmed from the older monolithic EDA scripts.
"""

from __future__ import annotations

import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# --- Regex / parsing helpers ---

REGEX_READING = re.compile(
    r"CB100-(?P<serial>\d+)-->.*?TS:\s*(?P<sec>\d+)\.(?P<ms>\d+)"
    r".*?Pulse:\s*(?P<pulse>\d+).*?Charge:\s*(?P<charge>\d+)"
    r"(?:.*?ADC:\s*(?P<adc>\d+))?",
    re.IGNORECASE,
)
REGEX_BRACKET_TIME = re.compile(r"\[(?P<hms>\d{2}:\d{2}:\d{2}\.\d{3})\]")
REGEX_TEMP_HEADER = re.compile(r"Temperature Information from\s+(CB100-\d+)\s*:", re.IGNORECASE)
REGEX_AMBIENT_TEMP = re.compile(r"Ambient Temperature:\s*([+-]?\d+(?:\.\d+)?)\s*°?C", re.IGNORECASE)
REGEX_DATE_IN_NAME = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

# Warning / event lines emitted by CB100_BLE.py (used to infer dropout reason)
REGEX_PC_RECEPTION_TIMEOUT = re.compile(
    r"^\[(?P<hms>\d{2}:\d{2}:\d{2}\.\d{3})\]\s*WARNING:\s*(?P<device>CB100-\d+)\s*\[[0-9A-Fa-f:]+\]\s*-\s*No data received for\s*(?P<secs>[\d.]+)\s*seconds",
    re.IGNORECASE,
)
REGEX_DATA_LOSS_DETECTED = re.compile(
    r"^\[(?P<hms>\d{2}:\d{2}:\d{2}\.\d{3})\]\s*DATA_LOSS_DETECTED:\s*(?P<device>CB100-\d+)\s*-\s*(?P<jump_s>\d+)\s*s\s+jump\s+detected",
    re.IGNORECASE,
)
# DATA_TIMEOUT is emitted by the dialog monitor in CB100_BLE.py:
#   DATA_TIMEOUT: CB100-259xxxx - No data for 0.4s at 11:22:33.123
REGEX_DATA_TIMEOUT = re.compile(
    r"^\[(?P<hms>\d{2}:\d{2}:\d{2}\.\d{3})\]\s*DATA_TIMEOUT:\s*(?P<device>CB100-\d+)\s*-\s*No data for\s*(?P<secs>[\d.]+)\s*s",
    re.IGNORECASE,
)
REGEX_KEEP_ALIVE_HEADER = re.compile(
    r"^\[(?P<hms>\d{2}:\d{2}:\d{2}\.\d{3})\]\s*Status/Keep-Alive Message from\s+(?P<device>CB100-\d+)\s*:\s*$",
    re.IGNORECASE,
)
REGEX_KEEP_ALIVE_PULSE = re.compile(r"Pulse Count:\s*(?P<pulse>\d+)", re.IGNORECASE)
REGEX_KEEP_ALIVE_CHARGE = re.compile(r"Charge Count:\s*(?P<charge>\d+)", re.IGNORECASE)
REGEX_KEEP_ALIVE_ADC = re.compile(r"ADC Value:\s*(?P<adc>\d+)", re.IGNORECASE)


@dataclass(frozen=True)
class AppConfig:
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
    show_plots: bool


class LogParser:
    """
    Discovers batch log files and parses:
    - readings (captured_at + counts)
    - temperature blocks (ambient temperature)
    - optional dropout-related events/warnings (PC timeout lines, DATA_LOSS_DETECTED, keep-alive all zeros)
    """

    def __init__(self, root: Path, pattern: str):
        self.root = root
        self.pattern = pattern
        self.logger = logging.getLogger(__name__)

    def parse(self, *, include_events: bool = False) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]] | Tuple[
        List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]
    ]:
        files = self._discover_files()
        self.logger.info("Discovered %d candidate .txt file(s)", len(files))

        all_readings: List[Dict[str, Any]] = []
        all_temps: List[Dict[str, Any]] = []
        all_events: List[Dict[str, Any]] = []

        for f_path in files:
            self.logger.info("Parsing %s", f_path.name)
            base_date = self._determine_base_date(f_path)
            try:
                r, t, e = self._parse_single_file(f_path, base_date, include_events=include_events)
                all_readings.extend(r)
                all_temps.extend(t)
                if include_events:
                    all_events.extend(e)
            except OSError:
                self.logger.exception("Failed reading %s", f_path)

        if include_events:
            return all_readings, all_temps, all_events
        return all_readings, all_temps

    def _discover_files(self) -> List[Path]:
        candidates: List[Path] = []
        for p in self.root.glob(self.pattern):
            if p.is_file() and p.suffix.lower() == ".txt" and "batch" in p.name.lower():
                candidates.append(p)
        return sorted(candidates, key=lambda p: str(p).lower())

    def _determine_base_date(self, f_path: Path) -> datetime:
        m = REGEX_DATE_IN_NAME.search(f_path.name)
        if m:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        dt = datetime.fromtimestamp(f_path.stat().st_mtime)
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    def _parse_single_file(
        self, f_path: Path, base_date: datetime, *, include_events: bool
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        readings: List[Dict[str, Any]] = []
        temps: List[Dict[str, Any]] = []
        events: List[Dict[str, Any]] = []

        with open(f_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = list(f)

        i = 0
        while i < len(lines):
            line = lines[i].strip()

            # 0) Keep-alive blocks (multi-line)
            if include_events:
                keep_m = REGEX_KEEP_ALIVE_HEADER.match(line)
                if keep_m:
                    parsed_ev, offset = self._extract_keep_alive_block(lines, i, base_date, f_path.name)
                    if parsed_ev:
                        events.append(parsed_ev)
                    i += offset
                    continue

                # 0b) Warning / event lines
                m_timeout = REGEX_PC_RECEPTION_TIMEOUT.match(line)
                if m_timeout:
                    log_time = self._parse_bracket_time(line, base_date)
                    try:
                        secs = float(m_timeout.group("secs"))
                    except ValueError:
                        secs = float("nan")
                    events.append(
                        {
                            "source_file": f_path.name,
                            "device_uid": m_timeout.group("device"),
                            "log_time": log_time,
                            "event_type": "pc_reception_timeout",
                            "duration_seconds": secs,
                            "raw_line": line,
                        }
                    )
                    i += 1
                    continue

                m_loss = REGEX_DATA_LOSS_DETECTED.match(line)
                if m_loss:
                    log_time = self._parse_bracket_time(line, base_date)
                    try:
                        jump_s = int(m_loss.group("jump_s"))
                    except ValueError:
                        jump_s = -1
                    events.append(
                        {
                            "source_file": f_path.name,
                            "device_uid": m_loss.group("device"),
                            "log_time": log_time,
                            "event_type": "data_loss_detected",
                            "jump_seconds": jump_s,
                            "raw_line": line,
                        }
                    )
                    i += 1
                    continue

                m_dt = REGEX_DATA_TIMEOUT.match(line)
                if m_dt:
                    log_time = self._parse_bracket_time(line, base_date)
                    try:
                        secs = float(m_dt.group("secs"))
                    except ValueError:
                        secs = float("nan")
                    events.append(
                        {
                            "source_file": f_path.name,
                            "device_uid": m_dt.group("device"),
                            "log_time": log_time,
                            "event_type": "data_timeout",
                            "duration_seconds": secs,
                            "raw_line": line,
                        }
                    )
                    i += 1
                    continue

            # 1) Temperature block
            temp_match = REGEX_TEMP_HEADER.search(line)
            if temp_match:
                parsed_temp, offset = self._extract_temperature_block(lines, i, temp_match, base_date, f_path.name)
                if parsed_temp:
                    temps.append(parsed_temp)
                i += offset
                continue

            # 2) Reading line
            reading_match = REGEX_READING.search(line)
            if reading_match:
                readings.append(self._extract_reading(line, reading_match, base_date, f_path.name))

            i += 1

        return readings, temps, events

    def _extract_keep_alive_block(
        self, lines: List[str], current_idx: int, base_date: datetime, file_name: str
    ) -> Tuple[Optional[Dict[str, Any]], int]:
        header_line = lines[current_idx].strip()
        m = REGEX_KEEP_ALIVE_HEADER.match(header_line)
        if not m:
            return None, 1

        device_uid = m.group("device")
        log_time = self._parse_bracket_time(header_line, base_date)

        pulse: Optional[int] = None
        charge: Optional[int] = None
        adc: Optional[int] = None

        lookahead = 18
        offset = 1
        for j in range(current_idx + 1, min(len(lines), current_idx + 1 + lookahead)):
            s = lines[j].strip()
            # break on next timestamped line
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

        # Only treat as keep-alive event if explicitly all zeros
        if log_time and pulse == 0 and charge == 0 and adc == 0:
            return (
                {
                    "source_file": file_name,
                    "device_uid": device_uid,
                    "log_time": log_time,
                    "event_type": "keep_alive_all_zeros",
                    "pulse_count": pulse,
                    "charge_count": charge,
                    "adc_value": adc,
                    "raw_line": header_line,
                },
                max(offset, 1),
            )

        return None, max(offset, 1)

    def _extract_temperature_block(
        self, lines: List[str], current_idx: int, header_match: re.Match[str], base_date: datetime, file_name: str
    ) -> Tuple[Optional[Dict[str, Any]], int]:
        dev_uid = header_match.group(1)
        log_time = self._parse_bracket_time(lines[current_idx], base_date)

        temp_val: Optional[float] = None
        for offset in range(1, 5):
            if current_idx + offset >= len(lines):
                break
            m_temp = REGEX_AMBIENT_TEMP.search(lines[current_idx + offset])
            if m_temp:
                try:
                    temp_val = float(m_temp.group(1))
                except ValueError:
                    temp_val = None
                break

        if log_time and temp_val is not None:
            return (
                {
                    "source_file": file_name,
                    "device_uid": dev_uid,
                    "log_time": log_time,
                    "temperature_c": temp_val,
                },
                1,
            )

        return None, 1

    def _extract_reading(self, line: str, match: re.Match[str], base_date: datetime, file_name: str) -> Dict[str, Any]:
        device_serial = f"CB100-{match.group('serial')}"
        ts_sec = int(match.group("sec"))
        ts_ms = int(match.group("ms"))
        pulse = int(match.group("pulse"))
        charge = int(match.group("charge"))
        adc_raw = match.groupdict().get("adc")
        adc = int(adc_raw) if adc_raw and adc_raw.isdigit() else 0

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
        m = REGEX_BRACKET_TIME.search(line)
        if not m:
            return None
        try:
            t = datetime.strptime(m.group("hms"), "%H:%M:%S.%f")
            return base_date.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=t.microsecond)
        except ValueError:
            return None


class DataProcessor:
    """
    DataFrame construction, cleaning, augmentation.
    """

    def __init__(self, logger: logging.Logger, *, lag_offset_seconds: float = -3600.0):
        self.logger = logger
        self.lag_offset_seconds = float(lag_offset_seconds)

    def process_raw_data(self, raw_readings: List[Dict[str, Any]], raw_temps: List[Dict[str, Any]]) -> pd.DataFrame:
        if not raw_readings:
            self.logger.warning("No readings found.")
            return pd.DataFrame()

        df = pd.DataFrame(raw_readings)
        for col in ["captured_at", "log_time", "created_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Temperature interpolation
        if raw_temps:
            t_df = pd.DataFrame(raw_temps)
            if not t_df.empty:
                t_df["log_time"] = pd.to_datetime(t_df["log_time"], errors="coerce")
                t_df["temperature_c"] = pd.to_numeric(t_df["temperature_c"], errors="coerce")
                df["temperature_c"] = self._interpolate_temperature(df, t_df)
            else:
                df["temperature_c"] = np.nan
        else:
            df["temperature_c"] = np.nan

        # Gateway lag (receiver time - device time + constant offset)
        if "log_time" in df.columns and "captured_at" in df.columns:
            df["lag_seconds_raw"] = (df["log_time"] - df["captured_at"]).dt.total_seconds()
            df["lag_seconds"] = df["lag_seconds_raw"] + float(self.lag_offset_seconds)

        return df

    def process_events(self, raw_events: List[Dict[str, Any]]) -> pd.DataFrame:
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
        for col in ["device_uid", "source_file"]:
            if col in ev.columns:
                ev[col] = ev[col].astype(str)
        return ev

    def filter_by_date(self, df: pd.DataFrame, date_str: str) -> Tuple[pd.DataFrame, str]:
        if df.empty or not date_str or str(date_str).lower() in {"all", "*"}:
            return df, "all"
        ts = pd.to_datetime(df["captured_at"], errors="coerce")
        mask = ts.dt.strftime("%Y-%m-%d") == str(date_str).strip()
        return df[mask].copy(), str(date_str).strip()

    def detect_intra_file_gaps(self, df: pd.DataFrame, threshold_ms: float) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        g = df.copy()
        g["captured_at"] = pd.to_datetime(g["captured_at"], errors="coerce")
        g = g.dropna(subset=["source_file", "device_uid", "captured_at"])
        if g.empty:
            return pd.DataFrame()

        g = g.sort_values(["source_file", "device_uid", "captured_at"])
        out_rows: List[Dict[str, Any]] = []
        for (sf, dev), gg in g.groupby(["source_file", "device_uid"], sort=False):
            if len(gg) < 2:
                continue
            deltas = gg["captured_at"].diff().dt.total_seconds() * 1000.0
            m = deltas > float(threshold_ms)
            if not bool(m.any()):
                continue
            for idx in gg.index[m]:
                curr = gg.loc[idx]
                delta = float(deltas.loc[idx])
                prev_ts = curr["captured_at"] - pd.Timedelta(milliseconds=delta)
                out_rows.append(
                    {
                        "device_uid": str(dev),
                        "source_file": str(sf),
                        "prev_captured_at": prev_ts,
                        "captured_at": curr["captured_at"],
                        "delta_ms": delta,
                        "threshold_ms": float(threshold_ms),
                    }
                )
        return pd.DataFrame(out_rows)

    def _interpolate_temperature(self, r_df: pd.DataFrame, t_df: pd.DataFrame) -> pd.Series:
        out = pd.Series(index=r_df.index, dtype=float)
        if r_df.empty or t_df.empty:
            return out
        need_r = {"source_file", "device_uid", "log_time"}
        need_t = {"source_file", "device_uid", "log_time", "temperature_c"}
        if not need_r.issubset(set(r_df.columns)) or not need_t.issubset(set(t_df.columns)):
            return out

        for (sf, dev), g_read in r_df.groupby(["source_file", "device_uid"], sort=False):
            g_temp = t_df[(t_df["source_file"] == sf) & (t_df["device_uid"] == dev)]
            if g_temp.empty or len(g_temp) < 2:
                continue
            x_target = pd.to_datetime(g_read["log_time"], errors="coerce").astype("int64") // 1_000_000_000
            x_ref = pd.to_datetime(g_temp["log_time"], errors="coerce").astype("int64") // 1_000_000_000
            y_ref = pd.to_numeric(g_temp["temperature_c"], errors="coerce").to_numpy(dtype=float)
            m = x_ref.notna().to_numpy() & np.isfinite(y_ref)
            if int(m.sum()) < 2:
                continue
            x_ref_v = x_ref.to_numpy(dtype=np.int64)[m]
            y_ref_v = y_ref[m]
            order = np.argsort(x_ref_v)
            x_ref_v = x_ref_v[order]
            y_ref_v = y_ref_v[order]
            try:
                interpolated = np.interp(
                    x_target.to_numpy(dtype=np.int64).astype(float),
                    x_ref_v.astype(float),
                    y_ref_v.astype(float),
                )
            except Exception:
                continue
            out.loc[g_read.index] = interpolated
        return out


class ResultsExporter:
    def __init__(self, output_dir: Path, logger: logging.Logger):
        self.output_dir = Path(output_dir)
        self.logger = logger
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_csv(self, df: pd.DataFrame, filename: str) -> Path:
        target = self.output_dir / filename
        final_path = self._write_safe(df, target)
        try:
            n = int(len(df))
        except Exception:
            n = -1
        self.logger.info("Exported CSV: %s (%d rows)", final_path, n)
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


def setup_logging(level_str: str, log_file: Optional[Path]) -> logging.Logger:
    level = getattr(logging, str(level_str).upper(), logging.INFO)
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )
    return logging.getLogger("cb100")


def load_real_gaps(real_gaps_dir: Path) -> pd.DataFrame:
    """
    Load curated gap CSVs from `real_gaps_dir`.

    Required columns:
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

    frames: List[pd.DataFrame] = []
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

