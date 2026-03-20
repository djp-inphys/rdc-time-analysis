#!/usr/bin/env python3
"""
thermal_drift.py

Focused analysis for thermal drift:
- computes lag_seconds (receiver log_time - device captured_at) with a configurable constant offset
- plots lag vs temperature and lag vs time per device
- exports per-device lag drift summary
- analyzes reporting interval (Δt) vs temperature bins (5°C by default), with permutation tests + BH-FDR
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from cb100_eda_lib import AppConfig, DataProcessor, LogParser, ResultsExporter, setup_logging


def _drift_s_per_hr(df: pd.DataFrame) -> float:
    """
    Estimate lag drift slope (seconds of lag change per hour) using a linear fit of lag vs receiver time.
    """
    if df.empty:
        return float("nan")
    x = pd.to_datetime(df["log_time"], errors="coerce")
    y = pd.to_numeric(df["lag_seconds"], errors="coerce")
    m = x.notna() & y.notna()
    if int(m.sum()) < 2:
        return float("nan")
    t0 = x[m].iloc[0]
    dt_s = (x[m] - t0).dt.total_seconds().to_numpy(dtype=float)
    lag_s = y[m].to_numpy(dtype=float)
    ok = np.isfinite(dt_s) & np.isfinite(lag_s)
    if int(ok.sum()) < 2:
        return float("nan")
    slope, _ = np.polyfit(dt_s[ok], lag_s[ok], deg=1)
    return float(slope) * 3600.0


def _plot_lag_vs_temp(device_df: pd.DataFrame, out_path: Path) -> None:
    g = device_df.copy()
    g = g[np.isfinite(pd.to_numeric(g["lag_seconds"], errors="coerce")) & np.isfinite(pd.to_numeric(g["temperature_c"], errors="coerce"))]
    if g.empty:
        return
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.scatter(g["temperature_c"], g["lag_seconds"], s=6, alpha=0.35)
    ax.set_title(f"{g['device_uid'].iloc[0]} - Lag vs Temperature")
    ax.set_xlabel("Temperature (°C)")
    ax.set_ylabel("Lag (seconds)")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _plot_lag_vs_time(device_df: pd.DataFrame, out_path: Path) -> None:
    g = device_df.copy()
    g["log_time"] = pd.to_datetime(g["log_time"], errors="coerce")
    g["lag_seconds"] = pd.to_numeric(g["lag_seconds"], errors="coerce")
    g = g.dropna(subset=["log_time", "lag_seconds"]).sort_values("log_time")
    if len(g) < 2:
        return
    slope = _drift_s_per_hr(g)
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(g["log_time"], g["lag_seconds"], lw=0.8)
    ax.set_title(f"{g['device_uid'].iloc[0]} - Lag vs Time (drift≈{slope:+.3f} s/hour)")
    ax.set_xlabel("Receiver time (log_time)")
    ax.set_ylabel("Lag (seconds)")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _plot_device_summary(device_df: pd.DataFrame, events_df: Optional[pd.DataFrame], *, gap_threshold_ms: float, out_path: Path) -> None:
    """
    3-panel summary plot similar to the old EDA output:
    - (1) Sequence: captured_at vs index
    - (2) Δt boxplot (within-file only), filtered to Δt <= gap_threshold_ms
    - (3) Charge vs time with dropout-reason overlays
    """
    df = device_df.copy()
    df["captured_at"] = pd.to_datetime(df["captured_at"], errors="coerce")
    df = df.dropna(subset=["captured_at", "source_file"]).sort_values("captured_at")
    if df.empty:
        return

    # Δt within-file only (avoid cross-file gaps)
    deltas = []
    for _, gf in df.groupby("source_file", sort=False):
        gfs = gf.sort_values("captured_at")
        d = gfs["captured_at"].diff().dt.total_seconds().mul(1000.0)
        d = pd.to_numeric(d, errors="coerce").dropna()
        if not d.empty:
            deltas.append(d)
    if not deltas:
        return
    all_deltas = pd.concat(deltas, ignore_index=True)
    valid_deltas = all_deltas[all_deltas <= float(gap_threshold_ms)]
    if valid_deltas.empty:
        return

    def _downsample_sorted(d: pd.DataFrame, max_points: int = 20000) -> pd.DataFrame:
        """
        Deterministic downsample that preserves ordering and includes endpoints.
        (Random sampling can drop the interesting region and breaks event-window filtering.)
        """
        if d is None or d.empty:
            return d
        n = int(len(d))
        if n <= int(max_points):
            return d
        idx = np.linspace(0, n - 1, int(max_points)).astype(int)
        return d.iloc[idx].copy()

    # Downsample for visualization if huge (but keep full df for event mapping/indexing)
    df_plot = _downsample_sorted(df, max_points=20000)

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 5))

    # (1) Sequence plot (plot downsampled line, but map event markers against full index series)
    ax1.plot(df_plot["captured_at"], range(len(df_plot)), lw=0.8)
    ax1.set_title(f"{df['device_uid'].iloc[0]} - Sequence")
    ax1.set_xlabel("Device time (captured_at)")
    ax1.set_ylabel("Index")

    # (2) Δt boxplot
    ax2.boxplot(valid_deltas.to_numpy(dtype=float), vert=True, patch_artist=True, boxprops=dict(facecolor="lightgray"))
    ax2.set_title(f"Δt (ms) < {float(gap_threshold_ms):.0f}")
    ax2.text(0.05, 0.95, f"Median: {float(np.median(valid_deltas.to_numpy(dtype=float))):.1f}ms", transform=ax2.transAxes, va="top")

    # (3) Charge vs time (use deterministic downsample so we don't invent weird line artifacts)
    if "charge_count" in df_plot.columns:
        y = pd.to_numeric(df_plot["charge_count"], errors="coerce")
        ax3.plot(df_plot["captured_at"], y, lw=0.8)
    ax3.set_title("Charge vs Time")
    ax3.set_xlabel("Device time (captured_at)")
    ax3.set_ylabel("Charge")

    # Event overlays
    if events_df is not None and not events_df.empty:
        mapped = _map_events_to_captured_time(device_df, events_df)
        if not mapped.empty:
            # Keep within the *full* device time window (+ small pad)
            t_min = df["captured_at"].min() - pd.Timedelta(seconds=5)
            t_max = df["captured_at"].max() + pd.Timedelta(seconds=5)
            mapped = mapped[(mapped["captured_at_est"] >= t_min) & (mapped["captured_at_est"] <= t_max)].copy()

        if not mapped.empty and "event_type" in mapped.columns:
            # Timeline markers (scatter on ax1)
            df_sorted = df.sort_values("captured_at")
            x = df_sorted["captured_at"].values

            def y_index_for_times(times: pd.Series) -> np.ndarray:
                return np.searchsorted(x, times.values, side="left")

            timeline_styles = {
                "data_loss_detected": dict(color="red", marker="v", label="DATA_LOSS_DETECTED"),
                "pc_reception_timeout": dict(color="purple", marker="x", label="PC Reception Timeout"),
                "data_timeout": dict(color="brown", marker="s", label="DATA_TIMEOUT"),
                "keep_alive_all_zeros": dict(color="orange", marker="o", label="Keep-Alive (all zeros)"),
            }
            for ev_type, style in timeline_styles.items():
                sub = mapped[mapped["event_type"] == ev_type]
                if sub.empty:
                    continue
                y_idx = y_index_for_times(sub["captured_at_est"])
                ax1.scatter(sub["captured_at_est"], y_idx, s=28, alpha=0.85, **style)
            ax1.legend(loc="best", fontsize=8, frameon=True)

            # Charge plot markers (vertical lines)
            charge_styles = {
                "data_loss_detected": dict(color="red", alpha=0.25, lw=1.2, label="DATA_LOSS_DETECTED"),
                "pc_reception_timeout": dict(color="purple", alpha=0.25, lw=1.2, label="PC Reception Timeout"),
                "data_timeout": dict(color="brown", alpha=0.25, lw=1.2, label="DATA_TIMEOUT"),
                "keep_alive_all_zeros": dict(color="orange", alpha=0.25, lw=1.2, label="Keep-Alive (all zeros)"),
            }
            used = set()
            for _, row in mapped.iterrows():
                ev_type = row.get("event_type")
                if ev_type not in charge_styles:
                    continue
                st = charge_styles[ev_type]
                label = st["label"] if ev_type not in used else None
                used.add(ev_type)
                ax3.axvline(row["captured_at_est"], color=st["color"], alpha=st["alpha"], lw=st["lw"], label=label)
            if used:
                ax3.legend(loc="best", fontsize=8, frameon=True)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _map_events_to_captured_time(readings_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Events are timestamped in receiver time (`log_time`). Our plots are in device time (`captured_at`).
    Map events to device time by finding nearest reading in log_time and subtracting lag_seconds.

    To avoid cross-file mismatches, we map within each (source_file, device_uid) group.
    """
    if readings_df.empty or events_df.empty:
        return pd.DataFrame()
    # Prefer raw lag for mapping PC time -> device time.
    # If we used corrected lag_seconds (which includes --lag-offset-seconds), we'd shift
    # events by the offset and lose alignment on the device-time axis.
    required_r = {"source_file", "device_uid", "log_time"}
    required_e = {"source_file", "device_uid", "log_time", "event_type"}
    if not required_r.issubset(set(readings_df.columns)) or not required_e.issubset(set(events_df.columns)):
        return pd.DataFrame()

    r_all = readings_df.copy()
    r_all["log_time"] = pd.to_datetime(r_all["log_time"], errors="coerce")
    if "lag_seconds_raw" in r_all.columns:
        r_all["_lag_for_mapping"] = pd.to_numeric(r_all["lag_seconds_raw"], errors="coerce")
    else:
        r_all["_lag_for_mapping"] = pd.to_numeric(r_all.get("lag_seconds"), errors="coerce")
    r_all = r_all.dropna(subset=["source_file", "device_uid", "log_time", "_lag_for_mapping"])

    e_all = events_df.copy()
    e_all["log_time"] = pd.to_datetime(e_all["log_time"], errors="coerce")
    e_all = e_all.dropna(subset=["source_file", "device_uid", "log_time"])

    if r_all.empty or e_all.empty:
        return pd.DataFrame()

    frames = []
    for (sf, dev), e in e_all.groupby(["source_file", "device_uid"], sort=False):
        r = r_all[(r_all["source_file"] == sf) & (r_all["device_uid"] == dev)].copy()
        if r.empty or e.empty:
            continue
        r = r.sort_values("log_time")[["log_time", "_lag_for_mapping"]]
        e = e.sort_values("log_time")
        merged = pd.merge_asof(
            e,
            r,
            on="log_time",
            direction="nearest",
            tolerance=pd.Timedelta(seconds=30),
        )
        merged["captured_at_est"] = merged["log_time"] - pd.to_timedelta(merged["_lag_for_mapping"], unit="s")
        merged = merged.dropna(subset=["captured_at_est"])
        if not merged.empty:
            frames.append(merged)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _plot_charge_vs_time_with_events(device_df: pd.DataFrame, events_df: pd.DataFrame, out_path: Path) -> None:
    g = device_df.copy()
    g["captured_at"] = pd.to_datetime(g["captured_at"], errors="coerce")
    g["charge_count"] = pd.to_numeric(g.get("charge_count"), errors="coerce")
    g = g.dropna(subset=["captured_at", "charge_count"]).sort_values("captured_at")
    if len(g) < 2:
        return

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(g["captured_at"], g["charge_count"], lw=0.8)
    ax.set_title(f"{g['device_uid'].iloc[0]} - Charge vs Time (with dropout-reason markers)")
    ax.set_xlabel("Device time (captured_at)")
    ax.set_ylabel("Charge")
    ax.grid(True, alpha=0.3)

    if events_df is not None and not events_df.empty:
        mapped = _map_events_to_captured_time(device_df, events_df)
        if not mapped.empty:
            # Keep only events that land within the plotted time window (+ a small pad)
            t_min = g["captured_at"].min() - pd.Timedelta(seconds=5)
            t_max = g["captured_at"].max() + pd.Timedelta(seconds=5)
            mapped = mapped[(mapped["captured_at_est"] >= t_min) & (mapped["captured_at_est"] <= t_max)].copy()

        if not mapped.empty and "event_type" in mapped.columns:
            styles = {
                "data_loss_detected": dict(color="red", alpha=0.28, lw=1.2, label="DATA_LOSS_DETECTED"),
                "pc_reception_timeout": dict(color="purple", alpha=0.28, lw=1.2, label="No data received (PC)"),
                "keep_alive_all_zeros": dict(color="orange", alpha=0.28, lw=1.2, label="Keep-Alive (all zeros)"),
            }
            used = set()
            for _, row in mapped.iterrows():
                ev_type = row.get("event_type")
                if ev_type not in styles:
                    continue
                st = styles[ev_type]
                label = st["label"] if ev_type not in used else None
                used.add(ev_type)
                ax.axvline(row["captured_at_est"], color=st["color"], alpha=st["alpha"], lw=st["lw"], label=label)
            if used:
                ax.legend(loc="best", fontsize=8, frameon=True)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Thermal drift analysis for CB100 batch logs.")
    p.add_argument("--root", default=".")
    p.add_argument("--pattern", default="**/*.txt")
    p.add_argument("--filter-date", default="all")
    p.add_argument("--export-dir", default="thermal_exports")
    p.add_argument("--plots-dir", default="thermal_plots")
    p.add_argument("--no-plots", action="store_true", default=False)
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--log-file", default=None)
    p.add_argument(
        "--lag-offset-seconds",
        type=float,
        default=-3600.0,
        help="Constant correction applied to lag_seconds: lag = (log_time - captured_at) + offset. Default: -3600.",
    )
    p.add_argument("--temp-bin-size-c", type=float, default=5.0, help="Temperature bin size (°C). Default: 5.")
    p.add_argument(
        "--temp-bin-test-alpha",
        type=float,
        default=0.05,
        help="Alpha for temperature-bin significance tests (BH-FDR on adjacent bin comparisons). Default: 0.05.",
    )
    p.add_argument(
        "--temp-bin-test-min-samples",
        type=int,
        default=200,
        help="Minimum samples required per bin to run adjacent-bin tests. Default: 200.",
    )
    p.add_argument(
        "--temp-bin-test-permutations",
        type=int,
        default=2000,
        help="Permutation count for median-difference tests between adjacent temperature bins. Default: 2000.",
    )
    p.add_argument(
        "--dt-max-ms",
        type=float,
        default=1000.0,
        help="Max Δt (ms) included in temperature-bin analysis (filters dropouts). Default: 1000.",
    )
    return p


def _bh_fdr(p_values: np.ndarray) -> np.ndarray:
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


def _perm_test_median(a: np.ndarray, b: np.ndarray, n_perm: int, seed: int = 0) -> float:
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
        stat = float(np.median(perm[:n_a]) - np.median(perm[n_a:]))
        if abs(stat) >= abs(obs):
            more_extreme += 1
    return float((more_extreme + 1) / (int(n_perm) + 1))


def _compute_drift_rate_s_per_hr(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute instantaneous drift rate as:
      drift_rate_s_per_hr = (Δlag_seconds / Δt_seconds) * 3600
    within each source_file to avoid cross-file gaps.
    """
    if df.empty:
        return pd.DataFrame()
    g = df.copy()
    g["log_time"] = pd.to_datetime(g["log_time"], errors="coerce")
    g["lag_seconds"] = pd.to_numeric(g["lag_seconds"], errors="coerce")
    g["temperature_c"] = pd.to_numeric(g.get("temperature_c"), errors="coerce")
    g = g.dropna(subset=["log_time", "lag_seconds", "temperature_c", "source_file"]).copy()
    if g.empty:
        return pd.DataFrame()

    rows = []
    for sf, gf in g.groupby("source_file", sort=False):
        gf = gf.sort_values("log_time")
        dt_s = gf["log_time"].diff().dt.total_seconds()
        dlag = gf["lag_seconds"].diff()
        # Typical cadence is ~0.2s; keep a generous window to filter weirdness
        ok = dt_s.notna() & dlag.notna() & (dt_s > 0.05) & (dt_s < 5.0)
        if not bool(ok.any()):
            continue
        rate = (dlag[ok] / dt_s[ok]) * 3600.0
        # Use the temperature at the "current" sample
        rows.append(
            pd.DataFrame(
                {
                    "source_file": sf,
                    "device_uid": gf.loc[ok, "device_uid"].astype(str).values,
                    "log_time": gf.loc[ok, "log_time"].values,
                    "temperature_c": gf.loc[ok, "temperature_c"].values,
                    "drift_rate_s_per_hr": rate.values,
                }
            )
        )
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out["temperature_c"] = pd.to_numeric(out["temperature_c"], errors="coerce")
    out["drift_rate_s_per_hr"] = pd.to_numeric(out["drift_rate_s_per_hr"], errors="coerce")
    return out.dropna(subset=["temperature_c", "drift_rate_s_per_hr"])


def _compute_dt_ms_vs_temp(df: pd.DataFrame, *, dt_max_ms: float) -> pd.DataFrame:
    """
    Build per-sample reporting interval dataset:
      dt_ms = diff(captured_at) within each (source_file, device_uid)
      temp   = temperature_c at the *current* sample (aligned with dt)

    This matches the intent in Thermal drift.md: reporting interval (Δt) vs temperature.
    """
    if df.empty:
        return pd.DataFrame()
    g = df.copy()
    g["captured_at"] = pd.to_datetime(g["captured_at"], errors="coerce")
    g["temperature_c"] = pd.to_numeric(g.get("temperature_c"), errors="coerce")
    g = g.dropna(subset=["source_file", "device_uid", "captured_at", "temperature_c"]).copy()
    if g.empty:
        return pd.DataFrame()

    rows = []
    for (sf, dev), gg in g.groupby(["source_file", "device_uid"], sort=False):
        gg = gg.sort_values("captured_at")
        dt_ms = gg["captured_at"].diff().dt.total_seconds().mul(1000.0)
        # align temp to current sample (same row as dt_ms)
        temp = gg["temperature_c"]
        m = dt_ms.notna() & temp.notna()
        if float(dt_max_ms) is not None and np.isfinite(float(dt_max_ms)):
            m = m & (dt_ms <= float(dt_max_ms))
        if not bool(m.any()):
            continue
        rows.append(
            pd.DataFrame(
                {
                    "source_file": sf,
                    "device_uid": dev,
                    "captured_at": gg.loc[m, "captured_at"].values,
                    "temperature_c": temp.loc[m].values,
                    "dt_ms": dt_ms.loc[m].values,
                }
            )
        )
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out["temperature_c"] = pd.to_numeric(out["temperature_c"], errors="coerce")
    out["dt_ms"] = pd.to_numeric(out["dt_ms"], errors="coerce")
    return out.dropna(subset=["temperature_c", "dt_ms"])


def _plot_dt_by_temp_bin_per_device(dt_df: pd.DataFrame, *, bin_size_c: float, out_path: Path) -> None:
    if dt_df.empty:
        return
    bin_size = float(bin_size_c)
    d = dt_df.copy()
    d["temp_bin"] = np.floor(d["temperature_c"] / bin_size) * bin_size
    bins = sorted(d["temp_bin"].dropna().unique())
    if not bins:
        return

    data = [d.loc[d["temp_bin"] == b, "dt_ms"].to_numpy(dtype=float) for b in bins]
    fig, ax = plt.subplots(figsize=(max(10, int(len(bins) * 0.65)), 5))
    ax.boxplot(data, tick_labels=[f"{b:.0f}-{b+bin_size:.0f}" for b in bins], showfliers=False)
    ax.set_title(f"{d['device_uid'].iloc[0]} - Reporting interval Δt by temperature bin")
    ax.set_xlabel("Temperature bin (°C)")
    ax.set_ylabel("Δt (ms)")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _dt_temp_bin_significance_all_tests(
    dt_df: pd.DataFrame, *, bin_size_c: float, min_samples: int, n_perm: int, alpha: float
) -> pd.DataFrame:
    """
    Adjacent-bin permutation tests on median(Δt) per device, then BH-FDR across *all* tests.
    """
    if dt_df.empty:
        return pd.DataFrame()
    bin_size = float(bin_size_c)
    df = dt_df.copy()
    df["temp_bin"] = np.floor(df["temperature_c"] / bin_size) * bin_size
    df["dt_ms"] = pd.to_numeric(df["dt_ms"], errors="coerce")
    df = df.dropna(subset=["temp_bin", "dt_ms", "device_uid"])
    if df.empty:
        return pd.DataFrame()

    rows = []
    for dev, g in df.groupby("device_uid", sort=True):
        bins = sorted(g["temp_bin"].unique())
        if len(bins) < 2:
            continue
        for b0, b1 in zip(bins[:-1], bins[1:]):
            a = g.loc[g["temp_bin"] == b0, "dt_ms"].to_numpy(dtype=float)
            b = g.loc[g["temp_bin"] == b1, "dt_ms"].to_numpy(dtype=float)
            a = a[np.isfinite(a)]
            b = b[np.isfinite(b)]
            if a.size < int(min_samples) or b.size < int(min_samples):
                p = float("nan")
            else:
                p = _perm_test_median(a, b, n_perm=int(n_perm), seed=0)
            rows.append(
                {
                    "device_uid": str(dev),
                    "bin0_start_c": float(b0),
                    "bin0_end_c": float(b0 + bin_size),
                    "bin1_start_c": float(b1),
                    "bin1_end_c": float(b1 + bin_size),
                    "n0": int(a.size),
                    "n1": int(b.size),
                    "median0_ms": float(np.median(a)) if a.size else float("nan"),
                    "median1_ms": float(np.median(b)) if b.size else float("nan"),
                    "median_diff_ms": float(np.median(b) - np.median(a)) if (a.size and b.size) else float("nan"),
                    "p_value": float(p),
                }
            )
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    pv = out["p_value"].to_numpy(dtype=float)
    pv_finite = np.array([x if np.isfinite(x) else 1.0 for x in pv], dtype=float)
    out["q_value_bh"] = _bh_fdr(pv_finite)
    out["significant"] = out["q_value_bh"] <= float(alpha)
    return out

def _plot_temp_bin_boxplot(df: pd.DataFrame, bin_size_c: float, out_path: Path) -> pd.DataFrame:
    """
    Boxplot drift_rate_s_per_hr per temperature bin; returns per-bin stats df.
    """
    if df.empty:
        return pd.DataFrame()
    bin_size = float(bin_size_c)
    d = df.copy()
    d["temp_bin"] = np.floor(d["temperature_c"] / bin_size) * bin_size
    bins = sorted(d["temp_bin"].dropna().unique())
    if not bins:
        return pd.DataFrame()

    data = [d.loc[d["temp_bin"] == b, "drift_rate_s_per_hr"].to_numpy(dtype=float) for b in bins]
    counts = [int(np.isfinite(x).sum()) for x in data]

    fig, ax = plt.subplots(figsize=(max(10, int(len(bins) * 0.6)), 5))
    ax.boxplot(data, tick_labels=[f"{b:.0f}-{b+bin_size:.0f}" for b in bins], showfliers=False)
    ax.set_title(f"Drift rate vs temperature bins (bin={bin_size:.1f}°C)")
    ax.set_xlabel("Temperature bin (°C)")
    ax.set_ylabel("Drift rate (s/hour)")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

    stats_rows = []
    for b, arr in zip(bins, data):
        arr = arr[np.isfinite(arr)]
        stats_rows.append(
            {
                "temp_bin_start_c": float(b),
                "temp_bin_end_c": float(b + bin_size),
                "n": int(arr.size),
                "median": float(np.median(arr)) if arr.size else float("nan"),
                "mean": float(np.mean(arr)) if arr.size else float("nan"),
            }
        )
    return pd.DataFrame(stats_rows)


def _temp_bin_significance(df: pd.DataFrame, bin_size_c: float, min_samples: int, n_perm: int, alpha: float) -> pd.DataFrame:
    """
    Adjacent-bin permutation tests (median difference) with BH-FDR.
    """
    if df.empty:
        return pd.DataFrame()
    bin_size = float(bin_size_c)
    d = df.copy()
    d["temp_bin"] = np.floor(d["temperature_c"] / bin_size) * bin_size
    bins = sorted(d["temp_bin"].dropna().unique())
    if len(bins) < 2:
        return pd.DataFrame()

    rows = []
    pvals = []
    for b0, b1 in zip(bins[:-1], bins[1:]):
        a = d.loc[d["temp_bin"] == b0, "drift_rate_s_per_hr"].to_numpy(dtype=float)
        b = d.loc[d["temp_bin"] == b1, "drift_rate_s_per_hr"].to_numpy(dtype=float)
        a = a[np.isfinite(a)]
        b = b[np.isfinite(b)]
        if a.size < int(min_samples) or b.size < int(min_samples):
            p = float("nan")
        else:
            p = _perm_test_median(a, b, n_perm=int(n_perm), seed=0)
        pvals.append(p)
        rows.append(
            {
                "bin0_start_c": float(b0),
                "bin0_end_c": float(b0 + bin_size),
                "bin1_start_c": float(b1),
                "bin1_end_c": float(b1 + bin_size),
                "n0": int(a.size),
                "n1": int(b.size),
                "median0": float(np.median(a)) if a.size else float("nan"),
                "median1": float(np.median(b)) if b.size else float("nan"),
                "median_diff": float(np.median(a) - np.median(b)) if (a.size and b.size) else float("nan"),
                "p_value": float(p),
            }
        )

    df_out = pd.DataFrame(rows)
    pv = np.array([x if np.isfinite(x) else 1.0 for x in pvals], dtype=float)
    df_out["q_value_bh"] = _bh_fdr(pv)
    df_out["significant"] = df_out["q_value_bh"] <= float(alpha)
    return df_out


def main() -> int:
    args = build_arg_parser().parse_args()
    cfg = AppConfig(
        root=Path(args.root).resolve(),
        pattern=str(args.pattern),
        filter_date=str(args.filter_date),
        export_dir=Path(args.export_dir),
        plots_dir=Path(args.plots_dir),
        gap_csv_dir=Path("gap_exports"),
        no_plots=bool(args.no_plots),
        log_level=str(args.log_level),
        log_file=(Path(args.log_file).resolve() if args.log_file else None),
        gap_threshold_ms=1000.0,
        show_plots=False,
    )
    logger = setup_logging(cfg.log_level, cfg.log_file)
    logger.info("Thermal drift analysis starting. lag_offset_seconds=%s", float(args.lag_offset_seconds))

    parser = LogParser(cfg.root, cfg.pattern)
    raw_readings, raw_temps, raw_events = parser.parse(include_events=True)
    processor = DataProcessor(logger, lag_offset_seconds=float(args.lag_offset_seconds))
    readings_df = processor.process_raw_data(raw_readings, raw_temps)
    events_df = processor.process_events(raw_events)
    analysis_df, tag = processor.filter_by_date(readings_df, cfg.filter_date)

    cfg.export_dir.mkdir(parents=True, exist_ok=True)
    cfg.plots_dir.mkdir(parents=True, exist_ok=True)
    exporter = ResultsExporter(cfg.export_dir, logger)

    # Per-device drift summary
    rows = []
    for dev, g in analysis_df.groupby("device_uid", sort=True):
        if g.empty:
            continue
        slope = _drift_s_per_hr(g)
        rows.append(
            {
                "device_uid": dev,
                "n_readings": int(len(g)),
                "lag_offset_seconds": float(args.lag_offset_seconds),
                "drift_s_per_hr": float(slope),
                "lag_seconds_median": float(pd.to_numeric(g["lag_seconds"], errors="coerce").median()),
                "temp_c_median": float(pd.to_numeric(g["temperature_c"], errors="coerce").median()),
            }
        )

        if not cfg.no_plots:
            safe = ResultsExporter.sanitize(str(dev))
            _plot_lag_vs_temp(g, cfg.plots_dir / f"{tag}_{safe}_lag_vs_temp.png")
            _plot_lag_vs_time(g, cfg.plots_dir / f"{tag}_{safe}_lag_vs_time.png")
            if events_df is not None and not events_df.empty:
                ev_g = events_df[events_df["device_uid"].astype(str) == str(dev)].copy()
                # Further restrict to the source files present in this device's analysis scope
                keep_files = set(g["source_file"].astype(str).unique())
                if keep_files and "source_file" in ev_g.columns:
                    ev_g = ev_g[ev_g["source_file"].astype(str).isin(keep_files)].copy()
                _plot_charge_vs_time_with_events(g, ev_g, cfg.plots_dir / f"{tag}_{safe}_charge_vs_time_events.png")
                _plot_device_summary(
                    g,
                    ev_g,
                    gap_threshold_ms=float(cfg.gap_threshold_ms),
                    out_path=cfg.plots_dir / f"{tag}_{safe}_summary.png",
                )

    summary_df = pd.DataFrame(rows).sort_values("device_uid") if rows else pd.DataFrame()
    exporter.write_csv(summary_df, f"{ResultsExporter.sanitize(tag)}_thermal_drift_summary.csv")

    # Temperature-bin analysis (boxplots + significance tests)
    safe_tag = ResultsExporter.sanitize(tag)

    # 1) Reporting interval Δt vs temperature bins (this matches Thermal drift.md)
    dt_df = _compute_dt_ms_vs_temp(analysis_df, dt_max_ms=float(args.dt_max_ms))
    if dt_df.empty:
        logger.info("No Δt samples available for temperature-bin analysis.")
    else:
        # Export per-device per-bin summaries
        bin_size = float(args.temp_bin_size_c)
        dt_df = dt_df.copy()
        dt_df["temp_bin"] = np.floor(dt_df["temperature_c"] / bin_size) * bin_size
        dt_bins = (
            dt_df.groupby(["device_uid", "temp_bin"], sort=True)["dt_ms"]
            .agg(["count", "median", "mean"])
            .reset_index()
            .rename(columns={"count": "n"})
        )
        exporter.write_csv(dt_bins, f"{safe_tag}_dt_temp_bins_per_device.csv")

        sig_dt = _dt_temp_bin_significance_all_tests(
            dt_df,
            bin_size_c=float(args.temp_bin_size_c),
            min_samples=int(args.temp_bin_test_min_samples),
            n_perm=int(args.temp_bin_test_permutations),
            alpha=float(args.temp_bin_test_alpha),
        )
        exporter.write_csv(sig_dt, f"{safe_tag}_dt_temp_bin_significance_all_devices.csv")

        if not cfg.no_plots:
            # Per-device boxplots (one file per device)
            for dev, g in dt_df.groupby("device_uid", sort=True):
                _plot_dt_by_temp_bin_per_device(
                    g,
                    bin_size_c=float(args.temp_bin_size_c),
                    out_path=cfg.plots_dir / f"{safe_tag}_{ResultsExporter.sanitize(str(dev))}_dt_by_temp_bin.png",
                )

    # 2) Lag drift-rate vs temperature bins (optional/diagnostic)
    drift_df = _compute_drift_rate_s_per_hr(analysis_df)
    if not drift_df.empty:
        stats_df = (
            drift_df.assign(temp_bin=(np.floor(drift_df["temperature_c"] / float(args.temp_bin_size_c)) * float(args.temp_bin_size_c)))
            .groupby("temp_bin", sort=True)["drift_rate_s_per_hr"]
            .agg(["count", "median", "mean"])
            .reset_index()
            .rename(columns={"count": "n"})
        )
        exporter.write_csv(stats_df, f"{safe_tag}_drift_rate_temp_bins.csv")
        sig_df = _temp_bin_significance(
            drift_df,
            bin_size_c=float(args.temp_bin_size_c),
            min_samples=int(args.temp_bin_test_min_samples),
            n_perm=int(args.temp_bin_test_permutations),
            alpha=float(args.temp_bin_test_alpha),
        )
        exporter.write_csv(sig_df, f"{safe_tag}_drift_rate_temp_bin_significance.csv")
        if not cfg.no_plots:
            _plot_temp_bin_boxplot(
                drift_df,
                bin_size_c=float(args.temp_bin_size_c),
                out_path=cfg.plots_dir / f"{safe_tag}_drift_rate_by_temp_bin.png",
            )

    logger.info("Thermal drift analysis complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

