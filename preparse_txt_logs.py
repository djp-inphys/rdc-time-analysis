#!/usr/bin/env python3
"""
preparse_txt_logs.py — CB100 Log Pre-Parser
============================================

PURPOSE
-------
Standalone preprocessing script for CB100 BLE log files (.txt).  Parses all
discovered log files **once**, writes the results to a compressed binary cache
(.pkl.gz), and optionally opens interactive per-file diagnostic plots.

The binary cache is consumed by ``eda-2.0.py`` via ``--parsed-binary-path`` to
skip slow text parsing on subsequent runs (important when files such as
``LongTimeOverNight_*.txt`` contain millions of lines).

WHY A SEPARATE SCRIPT?
----------------------
``eda-2.0.py`` can re-parse raw text on every invocation but that takes
minutes for large overnight captures.  By running this script first you get:

  1. A single compressed binary that loads in ~1-2 s instead of minutes.
  2. Interactive plots to visually inspect individual files before committing to
     a full analysis run.
  3. An ``inter_sample_ms`` column (gap between consecutive device samples) that
     is expensive to recompute and is now stored directly in the cache.

SUPPORTED LOG FORMATS
---------------------
Two log line styles are handled automatically:

  Bracket format (Batch*.txt produced by CB100_BLE.py):
    [13:23:16.919] CB100-2600577--> TS: 1766751796.303 | Pulse: 0 | Charge: 75 | ADC: 0 | ...

  CSV format (LongTimeOverNight*.txt / WeekendCapture*.txt — newer logger):
    Timestamp,Message
    17:01:11.575,CB100-2597625--> TS: 1774368070.079 | Pulse: 0 | Charge: 6 | ...

OUTPUTS
-------
Binary cache (default: ``parsed_cache/parsed_readings.pkl.gz``):
    Compressed pandas DataFrame (gzip pickle) containing:

    ============== ===========================================================
    Column         Description
    ============== ===========================================================
    source_file    Filename of the originating .txt log (basename only)
    device_uid     Device identifier, e.g. ``CB100-2597625``
    captured_at    Device-side timestamp (datetime, derived from TS field)
    log_time       PC/host reception timestamp (datetime, derived from line
                   prefix; NaT when not present)
    pulse_count    Pulse counter value at time of reading
    charge_count   Integrated charge counter value
    adc_value      Raw ADC reading (0 when not reported)
    inter_sample_ms Gap in milliseconds since the **previous** reading from
                   the same device in the same file; NaN for the first reading
                   in each (source_file, device_uid) group
    ============== ===========================================================

Interactive plots (per selected source file):
    Left  — Histogram of inter_sample_ms gaps (clipped at ``--max-gap-ms``)
    Right — Charge count vs. device time, one series per device

TYPICAL USAGE
-------------
Build cache for all captures, show all file plots::

    python preparse_txt_logs.py

Build cache and plot only overnight and weekend files::

    python preparse_txt_logs.py \\
        --plot-files "LongTimeOverNight*,WeekendCapture*"

Use a custom output path::

    python preparse_txt_logs.py --output-binary my_cache/parsed.pkl.gz

Skip plotting (cache only)::

    python preparse_txt_logs.py --plot-files ""

**Plot-only mode** — load an existing cache, skip all parsing and saving::

    python preparse_txt_logs.py \\
        --load-binary parsed_cache/parsed_readings.pkl.gz \\
        --plot-files "LongTimeOverNight*"

Load the cache back in Python::

    import pandas as pd
    df = pd.read_pickle("parsed_cache/parsed_readings.pkl.gz", compression="gzip")

"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Module-level logger — configured by setup_logging() at runtime
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Regular expressions
# ---------------------------------------------------------------------------
REGEX_READING = re.compile(
    r"CB100-(?P<serial>\d+)-->.*?TS:\s*(?P<sec>\d+)\.(?P<ms>\d+)"
    r".*?Pulse:\s*(?P<pulse>\d+).*?Charge:\s*(?P<charge>\d+)"
    r"(?:.*?ADC:\s*(?P<adc>\d+))?",
    re.IGNORECASE,
)
"""Matches a sensor-data reading line in either log format."""

REGEX_BRACKET_TIME = re.compile(r"\[(?P<hms>\d{2}:\d{2}:\d{2}\.\d{3})\]")
"""Matches the ``[HH:MM:SS.mmm]`` timestamp prefix used by Batch*.txt files."""

REGEX_CSV_TIME = re.compile(r"^(?P<hms>\d{2}:\d{2}:\d{2}\.\d{3}),")
"""Matches the ``HH:MM:SS.mmm,`` timestamp prefix in CSV-format logs."""

REGEX_DATE_IN_NAME = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
"""Extracts a YYYY-MM-DD date from a filename when present."""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Define and parse command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed argument namespace.
    """
    p = argparse.ArgumentParser(
        prog="preparse_txt_logs.py",
        description=(
            "Parse CB100 .txt log files, save a compressed binary cache, "
            "and display interactive per-file diagnostic plots."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python preparse_txt_logs.py\n"
            "  python preparse_txt_logs.py --plot-files 'LongTimeOverNight*,Weekend*'\n"
            "  python preparse_txt_logs.py --output-binary my_cache/out.pkl.gz\n"
            "  python preparse_txt_logs.py --plot-files ''  # cache only, no plots\n"
        ),
    )
    p.add_argument(
        "--root",
        default=".",
        help="Root directory to search for .txt files. Default: current directory.",
    )
    p.add_argument(
        "--pattern",
        default="rdc-captures/*.txt",
        help=(
            "Glob pattern (relative to --root) used to discover log files. "
            "Default: 'rdc-captures/*.txt'."
        ),
    )
    p.add_argument(
        "--output-binary",
        default="parsed_cache/parsed_readings.pkl.gz",
        help=(
            "Output path for the compressed binary cache (.pkl.gz). "
            "Parent directories are created automatically. "
            "Default: parsed_cache/parsed_readings.pkl.gz."
        ),
    )
    p.add_argument(
        "--plot-files",
        default="*",
        help=(
            "Comma-separated wildcard patterns for source_file names to include "
            "in interactive plots. Use '' to skip all plots. "
            "Example: 'LongTimeOverNight*,WeekendCapture*'. Default: '*' (all files)."
        ),
    )
    p.add_argument(
        "--max-gap-ms",
        type=float,
        default=300.0,
        help=(
            "Upper bound (ms) for the inter-sample gap histogram x-axis. "
            "Gaps larger than this value are still stored in the binary but "
            "are excluded from the plot view. Default: 300."
        ),
    )
    p.add_argument(
        "--load-binary",
        default="./parsed_cache/parsed_readings.pkl.gz",
        metavar="PATH",
        help=(
            "Load an existing .pkl.gz cache instead of parsing .txt files. "
            "When supplied, all parsing and saving steps are skipped — only "
            "the plots defined by --plot-files are produced. "
            "Example: --load-binary parsed_cache/parsed_readings.pkl.gz"
        ),
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity. Default: INFO.",
    )
    p.add_argument(
        "--log-file",
        default=None,
        help="Optional path to write log output to a file (in addition to stderr).",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(level_str: str, log_file: Optional[str] = None) -> None:
    """Configure root logger with a console handler and optional file handler.

    Parameters
    ----------
    level_str:
        One of ``'DEBUG'``, ``'INFO'``, ``'WARNING'``, ``'ERROR'``.
    log_file:
        Optional filesystem path.  When given, a ``FileHandler`` is added
        alongside the ``StreamHandler``.
    """
    level = getattr(logging, level_str.upper(), logging.INFO)
    fmt = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
        handlers.append(fh)

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, handlers=handlers, force=True)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def determine_base_date(file_path: Path) -> datetime:
    """Determine the calendar date for a log file to use as the timestamp base.

    The date is extracted from the filename when a ``YYYY-MM-DD`` pattern is
    present (e.g. ``Batch001_2025-12-26_10min.txt``).  If no date is found in
    the name, the file's modification-time date is used as a fallback.  The
    returned ``datetime`` always has ``hour=minute=second=microsecond=0``.

    Parameters
    ----------
    file_path:
        Path to the log file.

    Returns
    -------
    datetime
        Midnight on the file's calendar date.
    """
    m = REGEX_DATE_IN_NAME.search(file_path.name)
    if m:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    dt = datetime.fromtimestamp(file_path.stat().st_mtime)
    logger.debug(
        "No date in filename '%s'; using mtime date %s as base.",
        file_path.name,
        dt.strftime("%Y-%m-%d"),
    )
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_log_time(line: str, base_date: datetime) -> Optional[datetime]:
    """Extract the host-PC reception timestamp from a single log line.

    Tries the bracket format ``[HH:MM:SS.mmm]`` first (Batch logs), then the
    CSV prefix ``HH:MM:SS.mmm,`` (overnight/weekend logs).

    Parameters
    ----------
    line:
        A single stripped line from the log file.
    base_date:
        The calendar date (midnight) used to build a full ``datetime``.

    Returns
    -------
    datetime or None
        Fully resolved timestamp, or ``None`` if no recognisable pattern found.
    """
    m = REGEX_BRACKET_TIME.search(line)
    if not m:
        m = REGEX_CSV_TIME.match(line)
    if not m:
        return None
    try:
        t = datetime.strptime(m.group("hms"), "%H:%M:%S.%f")
        return base_date.replace(
            hour=t.hour, minute=t.minute, second=t.second, microsecond=t.microsecond
        )
    except ValueError:
        return None


def parse_txt_file(file_path: Path) -> Tuple[List[Dict[str, Any]], int]:
    """Parse a single CB100 log file and return extracted reading records.

    Processes the file line by line.  Each line is tested against
    ``REGEX_READING``; non-matching lines (headers, temperature blocks, event
    messages) are silently skipped.

    Parameters
    ----------
    file_path:
        Absolute or relative path to the .txt log file.

    Returns
    -------
    rows : list of dict
        One dict per matched reading line, with keys:
        ``source_file``, ``device_uid``, ``captured_at``, ``log_time``,
        ``pulse_count``, ``charge_count``, ``adc_value``.
    skipped : int
        Number of lines that did not match any reading pattern (informational).
    """
    base_date = determine_base_date(file_path)
    rows: List[Dict[str, Any]] = []
    skipped = 0

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            line = raw_line.strip()
            m = REGEX_READING.search(line)
            if not m:
                skipped += 1
                continue

            device_uid = f"CB100-{m.group('serial')}"
            ts_sec = int(m.group("sec"))
            ts_ms_part = int(m.group("ms"))
            pulse = int(m.group("pulse"))
            charge = int(m.group("charge"))
            adc_raw = m.groupdict().get("adc")
            adc = int(adc_raw) if adc_raw and adc_raw.isdigit() else 0

            captured_at = datetime.fromtimestamp(ts_sec + ts_ms_part / 1000.0)
            log_time = parse_log_time(line, base_date)

            rows.append(
                {
                    "source_file": file_path.name,
                    "device_uid": device_uid,
                    "captured_at": captured_at,
                    "log_time": log_time,
                    "pulse_count": pulse,
                    "charge_count": charge,
                    "adc_value": adc,
                }
            )

    return rows, skipped


# ---------------------------------------------------------------------------
# DataFrame enrichment
# ---------------------------------------------------------------------------

def add_inter_sample_ms(df: pd.DataFrame) -> pd.DataFrame:
    """Compute and attach per-device inter-sample gap times.

    For each ``(source_file, device_uid)`` group, sorts by ``captured_at``
    and computes the millisecond delta between consecutive readings.  The first
    reading in each group receives ``NaN`` (no previous sample to diff against).

    Gaps that span across different source files or devices are **not** computed
    (groups are isolated), so large overnight session boundaries do not pollute
    the distribution.

    Parameters
    ----------
    df:
        DataFrame produced by parsing; must contain ``source_file``,
        ``device_uid``, and ``captured_at`` columns.

    Returns
    -------
    pandas.DataFrame
        Same DataFrame with an additional ``inter_sample_ms`` column (float).
    """
    if df.empty:
        df["inter_sample_ms"] = pd.Series(dtype=float)
        return df

    d = df.sort_values(["source_file", "device_uid", "captured_at"]).copy()
    d["inter_sample_ms"] = (
        d.groupby(["source_file", "device_uid"])["captured_at"]
        .diff()
        .dt.total_seconds()
        * 1000.0
    )
    return d


# ---------------------------------------------------------------------------
# Binary cache I/O
# ---------------------------------------------------------------------------

def save_binary(df: pd.DataFrame, output_path: Path) -> None:
    """Persist a DataFrame to a gzip-compressed pickle file.

    The output directory is created if it does not exist.

    Uses ``pandas.DataFrame.to_pickle`` with ``compression='gzip'``.
    The resulting file can be reloaded with::

        df = pd.read_pickle(path, compression="gzip")

    Parameters
    ----------
    df:
        DataFrame to serialise.
    output_path:
        Absolute destination path (must end with ``.pkl.gz`` by convention).
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(output_path, compression="gzip")
    size_mb = output_path.stat().st_size / 1_048_576
    logger.info("Saved binary cache: %s (%.1f MB, %d rows)", output_path, size_mb, len(df))


def load_binary(cache_path: Path) -> pd.DataFrame:
    """Load a previously saved gzip-compressed pickle cache.

    Validates that the file exists before attempting to read it and logs
    basic statistics about the loaded DataFrame for quick sanity-checking.

    Parameters
    ----------
    cache_path:
        Path to the ``.pkl.gz`` file written by :func:`save_binary`.

    Returns
    -------
    pandas.DataFrame
        The deserialised DataFrame, exactly as it was when saved.

    Raises
    ------
    FileNotFoundError
        If ``cache_path`` does not exist.
    """
    if not cache_path.exists():
        raise FileNotFoundError(
            f"Binary cache not found: {cache_path}\n"
            "Run without --load-binary first to build the cache from .txt files."
        )
    size_mb = cache_path.stat().st_size / 1_048_576
    logger.info("Loading binary cache: %s (%.1f MB on disk) ...", cache_path, size_mb)
    t0 = time.monotonic()
    df = pd.read_pickle(cache_path, compression="gzip")
    elapsed = time.monotonic() - t0
    device_count = df["device_uid"].nunique() if "device_uid" in df.columns else 0
    file_count = df["source_file"].nunique() if "source_file" in df.columns else 0
    logger.info(
        "Cache loaded in %.2f s: %d rows, %d unique device(s), %d source file(s).",
        elapsed,
        len(df),
        device_count,
        file_count,
    )
    return df


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def plot_per_file(df: pd.DataFrame, file_name: str, max_gap_ms: float) -> None:
    """Render a side-by-side diagnostic figure for one source file.

    Left panel — **Inter-sample Gap Histogram**:
        Distribution of ``inter_sample_ms`` values, binned at 120 bars.
        Values above ``max_gap_ms`` are excluded so the axis is not dominated
        by rare overnight-gap outliers.

    Right panel — **Charge vs Time**:
        One line per device, plotting cumulative ``charge_count`` against
        ``captured_at`` device time.  Useful for spotting dropout events
        (flat sections), resets (step-downs), and comparing devices that share
        a file.

    The figure is created but *not* shown; call ``plt.show()`` after all
    figures are built so they appear together.

    Parameters
    ----------
    df:
        Full parsed DataFrame (all files).  The function filters to
        ``file_name`` internally.
    file_name:
        ``source_file`` value to plot (e.g. ``"LongTimeOverNight_4Units_576_625_267_608.txt"``).
    max_gap_ms:
        Upper clip for the histogram x-axis (does not affect stored data).
    """
    g = df[df["source_file"] == file_name].copy()
    if g.empty:
        logger.warning("plot_per_file: no rows found for '%s'; skipping.", file_name)
        return

    g = g.sort_values("captured_at")
    n_devices = g["device_uid"].nunique()
    n_readings = len(g)
    logger.debug("Plotting '%s': %d devices, %d readings.", file_name, n_devices, n_readings)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle(file_name, fontsize=10, y=1.01)

    # --- Left: inter-sample histogram ---
    inter = pd.to_numeric(g["inter_sample_ms"], errors="coerce")
    inter_valid = inter[np.isfinite(inter) & (inter >= 0)]
    if np.isfinite(max_gap_ms) and max_gap_ms > 0:
        inter_clipped = inter_valid[inter_valid <= float(max_gap_ms)]
    else:
        inter_clipped = inter_valid

    n_excluded = len(inter_valid) - len(inter_clipped)
    if len(inter_clipped) > 0:
        ax1.hist(inter_clipped, bins=120, color="steelblue", alpha=0.8)
    ax1.set_title(f"Inter-sample Gap Histogram\n(≤{max_gap_ms:.0f} ms, {n_excluded} gaps excluded)")
    ax1.set_xlabel("inter_sample_ms")
    ax1.set_ylabel("count")
    ax1.grid(True, alpha=0.25)
    if len(inter_clipped) > 0:
        ax1.axvline(float(inter_clipped.median()), color="red", lw=1.2, linestyle="--",
                    label=f"median {inter_clipped.median():.1f} ms")
        ax1.legend(fontsize=8)

    # --- Right: charge vs time ---
    if "charge_count" in g.columns:
        for dev, gd in g.groupby("device_uid", sort=True):
            ax2.plot(gd["captured_at"], gd["charge_count"], lw=0.8, label=str(dev))
        if n_devices <= 8:
            ax2.legend(loc="best", fontsize=8, frameon=True)
    ax2.set_title(f"Charge vs Time ({n_devices} device(s), {n_readings:,} readings)")
    ax2.set_xlabel("captured_at (device time)")
    ax2.set_ylabel("charge_count")
    ax2.grid(True, alpha=0.25)

    fig.tight_layout()
    plt.show()
    plt.savefig(f"plots/{file_name}.png")


def should_plot(file_name: str, patterns: List[str]) -> bool:
    """Return ``True`` if ``file_name`` matches any of the given glob patterns.

    Uses ``pathlib.Path.match`` which supports ``*`` and ``**`` wildcards.

    Parameters
    ----------
    file_name:
        Basename of the source file to test.
    patterns:
        List of glob pattern strings (e.g. ``['LongTimeOverNight*', 'Weekend*']``).
    """
    for pat in patterns:
        if Path(file_name).match(pat):
            return True
    return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    """Main entry point.

    Supports two operating modes selected by the presence of ``--load-binary``:

    **Parse mode** (default):
        Discovers and parses all ``.txt`` log files, builds a DataFrame,
        computes ``inter_sample_ms``, writes the binary cache, then plots.

    **Plot-only mode** (``--load-binary PATH``):
        Loads an existing ``.pkl.gz`` cache and jumps straight to plotting.
        No ``.txt`` files are read and no new cache is written.

    Returns
    -------
    int
        Exit code: 0 on success, 1 on error.
    """
    args = parse_args()
    setup_logging(args.log_level, args.log_file)

    t_start = time.monotonic()

    # ------------------------------------------------------------------
    # PLOT-ONLY MODE: load an existing binary cache, skip all parsing
    # ------------------------------------------------------------------
    if args.load_binary:
        cache_path = Path(args.load_binary)
        if not cache_path.is_absolute():
            cache_path = (Path.cwd() / cache_path).resolve()
        logger.info("Mode: plot-only (loading from binary cache)")
        try:
            df = load_binary(cache_path)
        except FileNotFoundError as exc:
            logger.error("%s", exc)
            return 1

        raw_patterns = [x.strip() for x in str(args.plot_files).split(",") if x.strip()]
        patterns = raw_patterns if raw_patterns else []
        if not patterns:
            logger.info("--plot-files is empty; nothing to plot.")
        else:
            selected_files = [
                f
                for f in sorted(df["source_file"].dropna().astype(str).unique())
                if should_plot(f, patterns)
            ]
            logger.info("Generating plots for %d file(s): %s", len(selected_files), selected_files)
            for fname in selected_files:
                plot_per_file(df, fname, max_gap_ms=float(args.max_gap_ms))
            if selected_files:
                plt.show()
            else:
                logger.warning("No source files matched --plot-files patterns: %s", patterns)

        elapsed_total = time.monotonic() - t_start
        logger.info("Done. Total elapsed: %.1f s", elapsed_total)
        return 0

    # ------------------------------------------------------------------
    # PARSE MODE: discover txt files → parse → save cache → plot
    # ------------------------------------------------------------------
    logger.info("Mode: parse + cache + plot")
    root = Path(args.root).resolve()
    logger.info("Root directory  : %s", root)
    logger.info("Glob pattern    : %s", args.pattern)

    files = sorted(
        [p for p in root.glob(args.pattern) if p.is_file() and p.suffix.lower() == ".txt"]
    )
    logger.info("Discovered %d txt file(s).", len(files))
    if not files:
        logger.error(
            "No .txt files found matching '%s' in '%s'. Check --root and --pattern.",
            args.pattern,
            root,
        )
        return 1

    # --- Parse all files ---
    all_rows: List[Dict[str, Any]] = []
    for i, p in enumerate(files, start=1):
        t_file = time.monotonic()
        rows, skipped = parse_txt_file(p)
        elapsed_ms = (time.monotonic() - t_file) * 1000
        logger.info(
            "[%d/%d] %-65s  %7d readings  %7d non-reading lines  %.0f ms",
            i,
            len(files),
            p.name,
            len(rows),
            skipped,
            elapsed_ms,
        )
        all_rows.extend(rows)

    if not all_rows:
        logger.error("No readings found across any file. Nothing to save.")
        return 1

    # --- Build DataFrame ---
    logger.info("Building DataFrame from %d raw records ...", len(all_rows))
    df = pd.DataFrame(all_rows)

    for col in ["captured_at", "log_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in ["pulse_count", "charge_count", "adc_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    device_count = df["device_uid"].nunique()
    file_count = df["source_file"].nunique()
    logger.info(
        "DataFrame built: %d rows, %d unique device(s) across %d source file(s).",
        len(df),
        device_count,
        file_count,
    )

    # --- Compute inter-sample times ---
    logger.info("Computing inter_sample_ms ...")
    df = add_inter_sample_ms(df)
    valid_gaps = df["inter_sample_ms"].dropna()
    logger.info(
        "inter_sample_ms summary: median=%.1f ms  p95=%.1f ms  max=%.1f ms  NaN=%d",
        float(valid_gaps.median()) if len(valid_gaps) else float("nan"),
        float(valid_gaps.quantile(0.95)) if len(valid_gaps) else float("nan"),
        float(valid_gaps.max()) if len(valid_gaps) else float("nan"),
        int(df["inter_sample_ms"].isna().sum()),
    )

    # --- Save binary cache ---
    output_path = Path(args.output_binary)
    if not output_path.is_absolute():
        output_path = (Path.cwd() / output_path).resolve()
    save_binary(df, output_path)

    # --- Plotting ---
    raw_patterns = [x.strip() for x in str(args.plot_files).split(",") if x.strip()]
    patterns = raw_patterns if raw_patterns else []
    if not patterns:
        logger.info("--plot-files is empty; skipping all plots.")
    else:
        selected_files = [
            f
            for f in sorted(df["source_file"].dropna().astype(str).unique())
            if should_plot(f, patterns)
        ]
        logger.info("Generating plots for %d file(s): %s", len(selected_files), selected_files)
        for fname in selected_files:
            plot_per_file(df, fname, max_gap_ms=float(args.max_gap_ms))

        if selected_files:
            plt.show()
        else:
            logger.warning("No source files matched --plot-files patterns: %s", patterns)

    elapsed_total = time.monotonic() - t_start
    logger.info("Done. Total elapsed: %.1f s", elapsed_total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
