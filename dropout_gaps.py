#!/usr/bin/env python3
"""
dropout_gaps.py

Focused analysis for dropouts and gaps:
- detects within-file gaps in device time (captured_at) above a threshold
- optionally replaces detected gaps with curated CSVs from ./real-gaps
- ignores gaps above a max threshold (default 10s) per your workflow
- exports consolidated and per-device gap CSVs
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from cb100_eda_lib import AppConfig, DataProcessor, LogParser, ResultsExporter, load_real_gaps, setup_logging


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Dropout/gap analysis for CB100 batch logs.")
    p.add_argument("--root", default=".")
    p.add_argument("--pattern", default="**/*.txt")
    p.add_argument("--filter-date", default="all")
    p.add_argument("--gap-threshold-ms", type=float, default=400.0)
    p.add_argument("--max-gap-ms", type=float, default=10000.0, help="Ignore gaps larger than this (ms). Default: 10000.")
    p.add_argument("--gap-csv-dir", default="gap_exports")
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--log-file", default=None)
    p.add_argument("--use-real-gaps", action="store_true", default=False)
    p.add_argument("--real-gaps-dir", default="real-gaps")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    cfg = AppConfig(
        root=Path(args.root).resolve(),
        pattern=str(args.pattern),
        filter_date=str(args.filter_date),
        export_dir=Path("session_exports"),
        plots_dir=Path("plots"),
        gap_csv_dir=Path(args.gap_csv_dir),
        no_plots=True,
        log_level=str(args.log_level),
        log_file=(Path(args.log_file).resolve() if args.log_file else None),
        gap_threshold_ms=float(args.gap_threshold_ms),
        show_plots=False,
    )
    logger = setup_logging(cfg.log_level, cfg.log_file)
    logger.info("Dropout/gap analysis starting. threshold_ms=%.1f max_gap_ms=%.1f", cfg.gap_threshold_ms, float(args.max_gap_ms))

    parser = LogParser(cfg.root, cfg.pattern)
    raw_readings, raw_temps = parser.parse()
    processor = DataProcessor(logger, lag_offset_seconds=-3600.0)
    readings_df = processor.process_raw_data(raw_readings, raw_temps)
    analysis_df, tag = processor.filter_by_date(readings_df, cfg.filter_date)

    # Compute gaps (or load curated)
    if bool(args.use_real_gaps):
        gaps_df = load_real_gaps(Path(args.real_gaps_dir))
        if gaps_df.empty:
            logger.warning("--use-real-gaps set but no usable CSVs found in %s", args.real_gaps_dir)
        else:
            # Restrict to analysis scope (same files/devices)
            keep_files = set(analysis_df["source_file"].astype(str).unique())
            keep_devs = set(analysis_df["device_uid"].astype(str).unique())
            gaps_df = gaps_df[
                gaps_df["source_file"].astype(str).isin(keep_files)
                & gaps_df["device_uid"].astype(str).isin(keep_devs)
            ].copy()
            gaps_df["delta_ms"] = pd.to_numeric(gaps_df["delta_ms"], errors="coerce")
            gaps_df = gaps_df.dropna(subset=["delta_ms"])
            gaps_df = gaps_df[gaps_df["delta_ms"] > float(cfg.gap_threshold_ms)].copy()
    else:
        gaps_df = processor.detect_intra_file_gaps(analysis_df, cfg.gap_threshold_ms)

    # Ignore large gaps (> max-gap-ms)
    if not gaps_df.empty and "delta_ms" in gaps_df.columns:
        gaps_df["delta_ms"] = pd.to_numeric(gaps_df["delta_ms"], errors="coerce")
        gaps_df = gaps_df.dropna(subset=["delta_ms"]).copy()
        gaps_df = gaps_df[gaps_df["delta_ms"] <= float(args.max_gap_ms)].copy()

    if gaps_df.empty:
        logger.info("No gaps found after filtering.")
    else:
        logger.warning("Detected %d gaps (after filtering). Max delta_ms=%.1f", int(len(gaps_df)), float(gaps_df["delta_ms"].max()))

    exporter = ResultsExporter(cfg.gap_csv_dir, logger)
    safe_tag = ResultsExporter.sanitize(tag)

    # Export consolidated
    exporter.write_csv(gaps_df, f"{safe_tag}_gaps_all_devices.csv")

    # Export per device
    if not gaps_df.empty:
        for dev, g in gaps_df.groupby("device_uid", sort=True):
            dev_safe = ResultsExporter.sanitize(str(dev).split("-")[-1])
            exporter.write_csv(g, f"{safe_tag}_gaps_{dev_safe}.csv")

    logger.info("Dropout/gap analysis complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

