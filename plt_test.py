#!/usr/bin/env python3
"""
plt_test.py - Per-device timing distribution analysis
======================================================

PURPOSE
-------
Loads all parsed session CSVs (``session_exports/all_device_*.csv``) and
produces three outputs that together answer the question:
  "Which CB100 devices behave differently from healthy ones, and on which clock?"

The two clocks of interest are:
  - Device clock   : ``captured_at`` - the Unix timestamp embedded in each BLE
                     packet by the CB100 firmware (RC oscillator, temperature-
                     dependent drift of ~+1 ms per +5 degrees C)
  - Host clock     : ``log_time`` - the PC wall-clock time at which the packet
                     was received (stable, but subject to OS scheduling jitter)

Before comparing inter-sample intervals on both clocks, a per-session median
offset is subtracted so that both timelines share the same epoch reference.

OUTPUTS
-------
device_timing_histograms.png
    Grid of 3 histograms per device:
      Row 1 - delta-t on device clock (dt_device_ms)
      Row 2 - delta-t on synced host clock (dt_host_ms)
      Row 3 - residual lag jitter after clock sync (residual_lag_ms, clipped to
              +/-200 ms to exclude multi-session drift artefacts)

device_timing_features.csv
    Table of per-device distribution statistics used for health discrimination:
      dev_median_ms, dev_iqr_ms, dev_skew, dev_timeout_ratio (adaptive), dev_p95_ms
      host_median_ms, host_iqr_ms, host_skew, host_timeout_ratio (adaptive)
      lag_jitter_std_ms, lag_jitter_p95_ms

device_health_features.png
    Three 2-D scatter plots of device features, coloured by known health status
    (Failing / At-Risk / New / Healthy).

ADAPTIVE TIMEOUT THRESHOLD
--------------------------
The old fixed 250 ms threshold misclassifies new devices whose nominal period
is ~258 ms.  This script uses 1.25 x per-device median instead, correctly
treating any interval more than 25% above the device's own typical period as
a "timeout" event.

CLOCK SYNC PROCEDURE
--------------------
For each (device_uid, source_file) session:
  1. Compute raw_lag = log_time_epoch - captured_at_epoch  (seconds)
  2. session_offset = median(raw_lag) for that session
  3. log_synced = log_time - session_offset
  4. residual_lag = log_synced - captured_at  (should be ~0 for healthy devices)

USAGE
-----
Run from the project root directory (where session_exports/ lives):

    python plt_test.py
    python plt_test.py --session-dir path/to/session_exports
    python plt_test.py --log-level DEBUG --log-file plt_test.log
    python plt_test.py --no-plots              # features CSV only
"""

from __future__ import annotations

import argparse
import glob
import logging
import sys
import time
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 – registers 3-D projection
import numpy as np
import pandas as pd
from scipy.stats import skew as scipy_skew
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA


# ---------------------------------------------------------------------------
# Module-level logger - configured by setup_logging() inside main()
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Device status registry (mirrors health-check.py)
# ---------------------------------------------------------------------------
FAILING_DEVICES = {'CB100-2600577', 'CB100-2598385', 'CB100-2599429'}
AT_RISK_DEVICES = {'CB100-2595836'}
NEW_DEVICES     = {'CB100-2597625', 'CB100-2598608', 'CB100-2599267'}

STATUS_COLOUR = {
    'Failing':  'red',
    'At-Risk':  'orange',
    'New':      'dodgerblue',
    'Healthy':  'green',
}
STATUS_MARKER = {
    'Failing':  'X',
    'At-Risk':  'D',
    'New':      's',
    'Healthy':  'o',
}

# Inter-sample gaps larger than this are treated as session boundaries and discarded.
# Overnight gaps between separate capture sessions can be hundreds of seconds;
# keeping them would dominate all statistics and histograms.
MAX_GAP_MS: float = 1_000.0

# Histogram bin count used across all panels.
HIST_BINS: int = 120

# Number of device columns in the per-device histogram grid.
GRID_COLS: int = 3


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Define and parse command-line arguments."""
    p = argparse.ArgumentParser(
        prog='plt_test.py',
        description='Per-device timing distribution analysis for CB100 devices.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  python plt_test.py\n'
            '  python plt_test.py --session-dir path/to/session_exports\n'
            '  python plt_test.py --no-plots\n'
            '  python plt_test.py --log-level DEBUG --log-file plt_test.log\n'
        ),
    )
    p.add_argument(
        '--session-dir',
        default='session_exports',
        help='Directory containing all_device_*.csv files. Default: session_exports.',
    )
    p.add_argument(
        '--output-dir',
        default='.',
        help='Directory for output PNG and CSV files. Default: current directory.',
    )
    p.add_argument(
        '--max-gap-ms',
        type=float,
        default=MAX_GAP_MS,
        help=(
            'Discard inter-sample gaps larger than this value (ms). '
            'Used to exclude session-boundary jumps. Default: 1000.'
        ),
    )
    p.add_argument(
        '--session-split-s',
        type=float,
        default=5.0,
        help=(
            'Device-clock gap threshold (seconds) used to detect monitoring-session '
            'boundaries within a single source file.  When consecutive readings from '
            'the same device are more than this many seconds apart the clock-sync '
            'offset is recomputed independently for each contiguous segment.  '
            'This prevents overnight multi-session files from polluting the residual '
            'lag calculation with day-scale drift.  Default: 5.'
        ),
    )
    p.add_argument(
        '--timeout-factor',
        type=float,
        default=1.25,
        help=(
            'Adaptive timeout threshold as a multiple of each device\'s median '
            'inter-sample time. Default: 1.25 (i.e. 25%% above median is a timeout).'
        ),
    )
    p.add_argument(
        '--no-plots',
        action='store_true',
        default=False,
        help='Skip all matplotlib output; write features CSV only.',
    )
    p.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging verbosity. Default: INFO.',
    )
    p.add_argument(
        '--log-file',
        default=None,
        help='Optional path to mirror log output to a file.',
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(level_str: str, log_file: str | None = None) -> None:
    """Configure root logger with a console handler and optional file handler."""
    level = getattr(logging, level_str.upper(), logging.INFO)
    fmt   = '%(asctime)s %(levelname)-8s %(name)s: %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file:
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
        handlers.append(fh)
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt,
                        handlers=handlers, force=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_status(uid: str) -> str:
    """Return the known health classification for a device UID."""
    if uid in FAILING_DEVICES:
        return 'Failing'
    if uid in AT_RISK_DEVICES:
        return 'At-Risk'
    if uid in NEW_DEVICES:
        return 'New'
    return 'Healthy'


# ---------------------------------------------------------------------------
# Section 1 - Data loading
# ---------------------------------------------------------------------------

def load_sessions(session_dir: str) -> pd.DataFrame:
    """Load and concatenate all per-device session CSV files.

    Only the columns required for timing analysis are read to keep memory
    usage proportional to the data size (6.5 M rows in production).

    Parameters
    ----------
    session_dir:
        Directory containing ``all_device_*.csv`` files.

    Returns
    -------
    pandas.DataFrame
        Combined DataFrame sorted by (device_uid, source_file, captured_at).
    """
    pattern = str(Path(session_dir) / 'all_device_*.csv')
    csv_files = sorted(glob.glob(pattern))
    if not csv_files:
        raise FileNotFoundError(
            f'No all_device_*.csv files found in "{session_dir}". '
            'Run from the project root or pass --session-dir.'
        )
    logger.info('Found %d session CSV file(s) in "%s".', len(csv_files), session_dir)

    frames = []
    for f in csv_files:
        frames.append(pd.read_csv(
            f,
            usecols=['device_uid', 'source_file', 'captured_at', 'log_time',
                     'charge_count', 'lag_seconds'],
            parse_dates=['captured_at', 'log_time'],
        ))
    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(['device_uid', 'source_file', 'captured_at'])
    logger.info('Loaded %d rows across %d device(s).', len(df), df['device_uid'].nunique())
    return df


# ---------------------------------------------------------------------------
# Section 2 - Session segmentation + inter-sample delta-t
# ---------------------------------------------------------------------------

def assign_segment_ids(df: pd.DataFrame, split_threshold_s: float) -> pd.DataFrame:
    """Label contiguous monitoring segments within each (device, source_file) group.

    Overnight and weekend log files contain multiple disconnected capture
    sessions separated by gaps of minutes to hours.  This function detects
    those breaks and assigns an integer ``segment_id`` that increments at
    each boundary, producing the composite key
    ``(device_uid, source_file, segment_id)`` which uniquely identifies each
    contiguous monitoring segment.

    .. note::
        **Why absolute clock-sync is abandoned**

        The ``captured_at`` timestamps in the session exports are derived from
        a device firmware Unix epoch value **converted with the log-file date
        as a fixed base**.  For captures that run past midnight the date does
        not advance in the conversion, so ``captured_at`` wraps back by
        exactly 86 400 s at midnight.  This means ``log_time - captured_at``
        (the raw lag) jumps by ±24 h once per day, making per-segment median
        subtraction unreliable regardless of how finely the file is split.

        The correct approach is to use ``lag_seconds.diff()``
        (packet-to-packet **change** in relative lag) which is immune to the
        wrap-around because the 24 h offset cancels in the difference.  See
        :func:`compute_timing_columns` for implementation.

    Parameters
    ----------
    df:
        DataFrame sorted by (device_uid, source_file, captured_at).
    split_threshold_s:
        Device-clock gap in seconds that triggers a new segment.  A value
        of 5 s comfortably separates normal ~0.2 s inter-sample periods from
        the multi-minute gaps between overnight capture sessions.

    Returns
    -------
    pandas.DataFrame
        Input DataFrame with a new ``segment_id`` integer column (0-based).
    """
    cap_diff_s = (
        df.groupby(['device_uid', 'source_file'], sort=False)['captured_at']
        .diff()
        .dt.total_seconds()
    )
    # NaN (first row of each group) and gaps above the threshold both mark
    # a new segment boundary.
    is_boundary = cap_diff_s.isna() | (cap_diff_s > split_threshold_s)

    # cumsum within each (device, file) group produces monotonically
    # increasing IDs that automatically reset at group boundaries.
    df['segment_id'] = (
        is_boundary.groupby(
            [df['device_uid'], df['source_file']], sort=False
        ).cumsum().astype(int) - 1   # make 0-based
    )
    return df


def compute_timing_columns(
    df: pd.DataFrame,
    max_gap_ms: float,
    session_split_s: float,
) -> pd.DataFrame:
    """Add segment_id, dt_device_ms, dt_host_ms, and dt_lag_ms columns.

    All timestamp arithmetic is performed on float64 epoch-second arrays or
    directly on the pre-computed ``lag_seconds`` column to avoid the overhead
    of pandas Timedelta objects across millions of rows.

    Columns added
    -------------
    segment_id : int
        Contiguous monitoring-session index within each (device, source_file)
        group.  Increments whenever the device-clock gap exceeds
        ``session_split_s``.

    dt_device_ms : float
        Packet-to-packet interval in milliseconds on the **device RC-oscillator
        clock** (derived from ``captured_at``).  Captures thermal clock drift
        and firmware scheduling jitter.

    dt_host_ms : float
        Packet-to-packet interval in milliseconds on the **host PC wall clock**
        (derived from ``log_time``).  Captures OS scheduling jitter and any
        BLE stack stalls.

    dt_lag_ms : float
        Packet-to-packet **change** in relative lag (ms), computed as::

            dt_lag_ms = diff(lag_seconds) * 1000

        This is the key jitter metric for residual lag analysis.  By
        differencing ``lag_seconds`` (rather than subtracting an estimated
        offset from the absolute lag) we avoid the midnight-wrap artefact
        that causes ``log_time - captured_at`` to jump by ±86 400 s in
        overnight files.  Healthy devices should have ``dt_lag_ms ≈ 0``
        (host and device clocks advance at the same rate between packets);
        problematic devices show large excursions.

    Why diff(lag_seconds) and not absolute lag after sync
    -------------------------------------------------------
    Diagnostic analysis revealed that ``captured_at`` in the session exports
    uses the log-filename date as a fixed base.  For captures that span
    midnight the date does not roll over, so ``captured_at`` wraps back by
    exactly 86 400 s.  Consequently ``log_time - captured_at`` has a range of
    exactly 24 h within each source file.  No segmentation threshold can
    reliably remove this artefact because the wrap is data-format-driven, not
    a function of session boundaries.  ``diff(lag_seconds)`` cancels the wrap
    identically because the 24 h offset appears in both consecutive readings
    and vanishes in the subtraction.

    Parameters
    ----------
    df:
        DataFrame from :func:`load_sessions` (must include ``lag_seconds``).
    max_gap_ms:
        Upper threshold (ms) for ``dt_device_ms`` and ``dt_host_ms``; rows
        with gaps above this are dropped to exclude session-boundary jumps.
    session_split_s:
        Forwarded to :func:`assign_segment_ids`.

    Returns
    -------
    pandas.DataFrame
        Filtered DataFrame with timing columns added.
    """
    # --- Step 0: assign segment IDs (kept for completeness; also used in
    # debug logging to report how many sessions are in each file) ---
    df = assign_segment_ids(df, split_threshold_s=session_split_s)

    n_segments = df.groupby(['device_uid', 'source_file', 'segment_id']).ngroups
    n_files    = df.groupby(['device_uid', 'source_file']).ngroups
    logger.info(
        'Segment detection (gap > %g s): %d file-device pairs split into %d segments '
        '(avg %.1f per pair).',
        session_split_s, n_files, n_segments, n_segments / max(n_files, 1),
    )
    if logger.isEnabledFor(logging.DEBUG):
        seg_counts = df.groupby(['device_uid', 'source_file'])['segment_id'].max() + 1
        multi = seg_counts[seg_counts > 1]
        if not multi.empty:
            logger.debug('File-device pairs with >1 segment:\n%s', multi.to_string())

    grp = df.groupby(['device_uid', 'source_file'], sort=False)

    # --- Step A: packet-to-packet interval on the device RC-oscillator clock ---
    df['dt_device_ms'] = grp['captured_at'].diff().dt.total_seconds().values * 1000.0

    # --- Step B: packet-to-packet interval on the host wall clock ---
    # diff() cancels any constant offset between the two clocks so no
    # explicit sync is needed here.
    df['dt_host_ms'] = (
        df['log_time'].astype('int64').values / 1e9
    )
    df['dt_host_ms'] = grp['dt_host_ms'].diff().values * 1000.0

    # --- Step C: packet-to-packet change in relative lag ---
    # lag_seconds = log_time - captured_at (offset-corrected, from session CSV).
    # diff() removes the constant per-session offset and the midnight-wrap
    # artefact simultaneously, leaving only genuine packet-reception jitter.
    df['dt_lag_ms'] = grp['lag_seconds'].diff().values * 1000.0

    # Drop first-row NaNs (from diff) and reject session-boundary jumps
    df = df.dropna(subset=['dt_device_ms', 'dt_host_ms', 'dt_lag_ms'])
    df = df[(df['dt_device_ms'] > 0) & (df['dt_device_ms'] <= max_gap_ms)]
    df = df[(df['dt_host_ms']   > 0) & (df['dt_host_ms']   <= max_gap_ms)]
    # dt_lag_ms can be negative (device clock momentarily faster than host);
    # reject only extreme values that are clearly session jumps.
    df = df[df['dt_lag_ms'].abs() <= max_gap_ms]

    logger.info('After gap filtering (%g ms max): %d rows remain.', max_gap_ms, len(df))
    return df


# ---------------------------------------------------------------------------
# Section 3 - Per-device histogram grid
# ---------------------------------------------------------------------------

def plot_histogram_grid(df: pd.DataFrame, output_path: Path) -> None:
    """Render and save the per-device 3-row histogram grid.

    Layout: GRID_COLS devices per page row, 3 sub-rows per device:
      Row 0 - device-clock delta-t
      Row 1 - host-clock delta-t (after session-offset sync)
      Row 2 - residual lag jitter clipped to +/-200 ms

    Parameters
    ----------
    df:
        DataFrame with dt_device_ms, dt_host_ms, residual_lag_ms columns.
    output_path:
        Destination path for the PNG file.
    """
    devices = sorted(df['device_uid'].unique())
    n_dev   = len(devices)
    rows    = (n_dev + GRID_COLS - 1) // GRID_COLS

    logger.info('Building histogram grid: %d devices, %dx%d layout.', n_dev, rows, GRID_COLS)

    fig, axes = plt.subplots(
        rows * 3, GRID_COLS,
        figsize=(GRID_COLS * 6, rows * 9),
        squeeze=False,
    )
    fig.suptitle(
        'Per-device timing distributions\n'
        '(top: device clock dt  |  mid: host clock dt  |  bot: residual lag)',
        fontsize=11, y=1.002,
    )

    def _hist(ax, data, median_val, title, xlabel, colour):
        """Draw a single histogram panel with a median marker."""
        ax.hist(data, bins=HIST_BINS, color=colour, alpha=0.75, edgecolor='none')
        ax.axvline(median_val, color='black', lw=1.3, linestyle='--',
                   label=f'median {median_val:.1f} ms')
        ax.set_title(title, fontsize=8)
        ax.set_xlabel(xlabel, fontsize=7)
        ax.set_ylabel('count', fontsize=7)
        ax.legend(fontsize=7, frameon=True)
        ax.grid(True, alpha=0.25)
        ax.tick_params(labelsize=7)

    for col_idx, device in enumerate(devices):
        row_block = (col_idx // GRID_COLS) * 3
        col       = col_idx % GRID_COLS
        status    = get_status(device)
        colour    = STATUS_COLOUR[status]
        label     = f'{device}\n[{status}]'

        g_dev  = df.loc[df['device_uid'] == device, 'dt_device_ms'].dropna()
        g_host = df.loc[df['device_uid'] == device, 'dt_host_ms'].dropna()
        g_lag  = df.loc[df['device_uid'] == device, 'dt_lag_ms'].dropna()

        _hist(axes[row_block,     col], g_dev,  float(g_dev.median()),
              f'{label}\nDevice clock dt',       'dt_device_ms',  colour)
        _hist(axes[row_block + 1, col], g_host, float(g_host.median()),
              f'{label}\nHost clock dt',         'dt_host_ms',    colour)

        # dt_lag_ms is centred near 0 (small packet-to-packet lag change).
        # Clip to +/-200 ms to keep the histogram readable; extreme values
        # caused by BLE stalls are visible as excluded-count annotations.
        lag_clip = g_lag[g_lag.abs() <= 200.0]
        lag_med  = float(lag_clip.median()) if len(lag_clip) else 0.0
        n_excl   = len(g_lag) - len(lag_clip)
        axes[row_block + 2, col].hist(lag_clip, bins=HIST_BINS,
                                      color='slategrey', alpha=0.75, edgecolor='none')
        axes[row_block + 2, col].axvline(lag_med, color='red', lw=1.3, linestyle='--',
                                         label=f'median {lag_med:.2f} ms')
        axes[row_block + 2, col].set_title(
            f'{label}\nLag change per packet (excl. {n_excl} jumps)', fontsize=8)
        axes[row_block + 2, col].set_xlabel('dt_lag_ms', fontsize=7)
        axes[row_block + 2, col].set_ylabel('count', fontsize=7)
        axes[row_block + 2, col].legend(fontsize=7, frameon=True)
        axes[row_block + 2, col].grid(True, alpha=0.25)
        axes[row_block + 2, col].tick_params(labelsize=7)

    # Hide leftover empty axes when device count is not a multiple of GRID_COLS
    for idx in range(n_dev, rows * GRID_COLS):
        row_block = (idx // GRID_COLS) * 3
        col       = idx % GRID_COLS
        for r in range(3):
            axes[row_block + r, col].set_visible(False)

    fig.tight_layout()
    fig.savefig(output_path, dpi=120, bbox_inches='tight')
    logger.info('Saved histogram grid: %s', output_path)


# ---------------------------------------------------------------------------
# Section 4 - Distribution feature table
# ---------------------------------------------------------------------------

def compute_features(df: pd.DataFrame, timeout_factor: float) -> pd.DataFrame:
    """Compute per-device distribution statistics for health discrimination.

    Uses an adaptive timeout threshold (``timeout_factor`` x per-device median)
    so that devices with different nominal periods (~204 ms batch devices vs
    ~258 ms overnight devices) are judged on the same relative scale.

    Parameters
    ----------
    df:
        DataFrame with dt_device_ms, dt_host_ms, residual_lag_ms columns.
    timeout_factor:
        Multiplier applied to each device's median to derive its timeout
        threshold.  Default 1.25 means any gap more than 25% above the
        device's own typical period counts as a timeout.

    Returns
    -------
    pandas.DataFrame
        One row per device, sorted by device-clock skewness (descending).
    """
    records = []
    for device, g in df.groupby('device_uid'):
        d  = g['dt_device_ms'].dropna()
        h  = g['dt_host_ms'].dropna()
        # dt_lag_ms: packet-to-packet change in relative lag.
        # Centred near 0; clip to ±200 ms to exclude BLE-stall outliers
        # from the summary statistics (they are counted separately).
        dl = g['dt_lag_ms'].dropna()
        dl_clean = dl[dl.abs() <= 200.0]

        dev_thresh  = timeout_factor * float(d.median())
        host_thresh = timeout_factor * float(h.median())

        records.append({
            'device':             device,
            'status':             get_status(device),
            'n_samples':          len(d),
            # Device-clock features
            'dev_median_ms':      round(float(d.median()), 2),
            'dev_iqr_ms':         round(float(d.quantile(0.75) - d.quantile(0.25)), 2),
            'dev_skew':           round(float(scipy_skew(d)), 3),
            'dev_timeout_ratio':  round(float((d > dev_thresh).mean()), 4),
            'dev_p95_ms':         round(float(d.quantile(0.95)), 2),
            # Host-clock features
            'host_median_ms':     round(float(h.median()), 2),
            'host_iqr_ms':        round(float(h.quantile(0.75) - h.quantile(0.25)), 2),
            'host_skew':          round(float(scipy_skew(h)), 3),
            'host_timeout_ratio': round(float((h > host_thresh).mean()), 4),
            # Lag-change jitter: how stable is the relative timing per packet?
            # Small std = host and device clocks advance together consistently.
            # Large std = OS scheduling anomalies or device BLE stalls.
            'lag_jitter_std_ms':  round(float(dl_clean.std()), 3),
            'lag_jitter_p95_ms':  round(float(dl_clean.abs().quantile(0.95)), 3),
            'lag_jitter_n_excl':  int(len(dl) - len(dl_clean)),
        })

    feat_df = pd.DataFrame(records).set_index('device')
    feat_df = feat_df.sort_values('dev_skew', ascending=False)
    return feat_df


# ---------------------------------------------------------------------------
# Section 5 - 3-D PCA scatter
# ---------------------------------------------------------------------------

# The four raw features fed into PCA.
PCA_FEATURES = ['dev_iqr_ms', 'dev_timeout_ratio', 'dev_skew', 'lag_jitter_std_ms']
PCA_LABELS   = ['IQR (ms)', 'Timeout Ratio', 'Skewness', 'Lag Jitter Std (ms)']


def plot_pca_3d(feat_df: pd.DataFrame, output_path: Path) -> None:
    """Reduce four health features to three PCA components and render a 3-D scatter.

    Feature space
    -------------
    The four input dimensions are:

    * ``dev_iqr_ms``         -- tightness of the device heartbeat (IQR of dt_device_ms)
    * ``dev_timeout_ratio``  -- fraction of intervals exceeding 1.25 x per-device median
    * ``dev_skew``           -- right-tail skew of dt_device_ms distribution
    * ``lag_jitter_std_ms``  -- std of packet-to-packet lag change (dt_lag_ms)

    All four are z-score standardised before PCA so that no single feature
    dominates due to scale differences.

    PCA projection
    --------------
    Three principal components capture the maximum variance in the four-
    dimensional feature space.  The axes are labelled with their explained
    variance fraction and the top contributing raw feature so the viewer
    can interpret the geometry without running the numbers separately.

    Two views are rendered side-by-side:
    * Left  -- PC1 / PC2 / PC3  (all devices, rotated for best separation)
    * Right -- Loading arrows: which raw features drive each PC

    Parameters
    ----------
    feat_df:
        Output of :func:`compute_features`.
    output_path:
        Destination path for the PNG file.
    """
    # --- Extract and scale the feature matrix ---
    X_raw = feat_df[PCA_FEATURES].values.astype(float)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_raw)

    pca = PCA(n_components=3)
    X_pca = pca.fit_transform(X_scaled)   # shape (n_devices, 3)

    ev = pca.explained_variance_ratio_

    # Label each PC with its dominant contributing feature
    loadings = pca.components_           # shape (3, 4)
    top_feat = [PCA_LABELS[int(np.argmax(np.abs(loadings[i])))] for i in range(3)]
    pc_labels = [
        f'PC{i+1} ({ev[i]*100:.1f}%)\n[{top_feat[i]}]'
        for i in range(3)
    ]

    logger.info(
        'PCA explained variance: PC1=%.1f%%  PC2=%.1f%%  PC3=%.1f%%  total=%.1f%%',
        ev[0]*100, ev[1]*100, ev[2]*100, ev.sum()*100,
    )
    for i, (pc_label, load_row) in enumerate(zip(pc_labels, loadings)):
        contrib = ', '.join(
            f'{PCA_LABELS[j]}={load_row[j]:+.2f}' for j in range(len(PCA_FEATURES))
        )
        logger.info('  %s  loadings: %s', pc_label.replace('\n', ' '), contrib)

    # --- Build figure: left=3-D scatter, right=loading biplot ---
    fig = plt.figure(figsize=(18, 8))
    fig.suptitle(
        '3-D PCA of device health features\n'
        f'(IQR, Timeout Ratio, Skewness, Lag Jitter)  --  '
        f'total variance explained: {ev.sum()*100:.1f}%',
        fontsize=11,
    )

    # -- Left panel: 3-D scatter --
    ax3d = fig.add_subplot(1, 2, 1, projection='3d')

    seen_labels: dict = {}
    for i, (device, row) in enumerate(feat_df.iterrows()):
        device = str(device)
        status = row['status']
        xs, ys, zs = X_pca[i, 0], X_pca[i, 1], X_pca[i, 2]
        sc = ax3d.scatter(
            xs, ys, zs,
            c=STATUS_COLOUR[status],
            marker=STATUS_MARKER[status],
            s=80, depthshade=True, zorder=3,
        )
        ax3d.text(xs, ys, zs, '  ' + device.replace('CB100-', ''),
                  fontsize=6, zorder=4)
        seen_labels.setdefault(status, sc)

    ax3d.set_xlabel(pc_labels[0], fontsize=8, labelpad=8)
    ax3d.set_ylabel(pc_labels[1], fontsize=8, labelpad=8)
    ax3d.set_zlabel(pc_labels[2], fontsize=8, labelpad=8)
    ax3d.set_title('3-D PCA projection', fontsize=9)

    # Manually build a legend from the seen_labels proxies
    legend_handles = [
        plt.Line2D([0], [0],
                   marker=STATUS_MARKER[s], color='w',
                   markerfacecolor=STATUS_COLOUR[s], markersize=8, label=s)
        for s in ['Failing', 'At-Risk', 'New', 'Healthy']
        if s in seen_labels
    ]
    ax3d.legend(handles=legend_handles, fontsize=8, loc='upper left')

    # -- Right panel: loading biplot (2-D, PC1 vs PC2) --
    ax2d = fig.add_subplot(1, 2, 2)

    # Project devices onto PC1/PC2
    for i, (device, row) in enumerate(feat_df.iterrows()):
        device = str(device)
        status = row['status']
        ax2d.scatter(
            X_pca[i, 0], X_pca[i, 1],
            c=STATUS_COLOUR[status],
            marker=STATUS_MARKER[status],
            s=70, zorder=3,
        )
        ax2d.annotate(
            device.replace('CB100-', ''),
            (X_pca[i, 0], X_pca[i, 1]),
            fontsize=6, ha='left', va='bottom',
            xytext=(3, 3), textcoords='offset points',
        )

    # Draw loading arrows scaled to the scatter extent
    scale = np.abs(X_pca[:, :2]).max() * 0.85
    for j, feat_label in enumerate(PCA_LABELS):
        lx, ly = loadings[0, j] * scale, loadings[1, j] * scale
        ax2d.annotate(
            '', xy=(lx, ly), xytext=(0, 0),
            arrowprops=dict(arrowstyle='->', color='dimgrey', lw=1.5),
        )
        ax2d.text(lx * 1.08, ly * 1.08, feat_label,
                  fontsize=7, color='dimgrey', ha='center', va='center')

    ax2d.axhline(0, color='lightgrey', lw=0.8, zorder=0)
    ax2d.axvline(0, color='lightgrey', lw=0.8, zorder=0)
    ax2d.set_xlabel(pc_labels[0].split('\n')[0], fontsize=8)
    ax2d.set_ylabel(pc_labels[1].split('\n')[0], fontsize=8)
    ax2d.set_title('PC1 vs PC2 with feature loadings', fontsize=9)
    ax2d.grid(True, alpha=0.25)
    ax2d.legend(handles=legend_handles, fontsize=8)

    fig.tight_layout()
    fig.savefig(output_path, dpi=120, bbox_inches='tight')
    logger.info('Saved PCA 3-D scatter: %s', output_path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    """Orchestrate all analysis sections and produce outputs.

    Returns
    -------
    int
        Exit code: 0 on success, 1 on error.
    """
    args = parse_args()
    setup_logging(args.log_level, args.log_file)

    t_start = time.monotonic()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Section 1 - Load session CSVs
    # ------------------------------------------------------------------
    try:
        df = load_sessions(args.session_dir)
    except FileNotFoundError as exc:
        logger.error('%s', exc)
        return 1

    # ------------------------------------------------------------------
    # Section 2 - Clock sync and inter-sample delta-t
    # ------------------------------------------------------------------
    logger.info(
        'Computing inter-sample delta-t and lag-change jitter '
        '(session split threshold: %g s, max gap: %g ms) ...',
        args.session_split_s, args.max_gap_ms,
    )
    df = compute_timing_columns(
        df,
        max_gap_ms=args.max_gap_ms,
        session_split_s=args.session_split_s,
    )

    # ------------------------------------------------------------------
    # Section 3 - Per-device histogram grid
    # ------------------------------------------------------------------
    if not args.no_plots:
        plot_histogram_grid(df, out_dir / 'device_timing_histograms.png')

    # ------------------------------------------------------------------
    # Section 4 - Distribution feature table
    # ------------------------------------------------------------------
    logger.info('Computing distribution features (timeout factor: %.2f x median) ...',
                args.timeout_factor)
    feat_df = compute_features(df, timeout_factor=args.timeout_factor)

    features_path = out_dir / 'device_timing_features.csv'
    feat_df.to_csv(features_path)
    logger.info('Saved features table: %s', features_path)

    logger.info('\nDistribution features (sorted by device-clock skew):\n%s', feat_df.to_string())

    # ------------------------------------------------------------------
    # Section 5 - 3-D PCA scatter
    # ------------------------------------------------------------------
    if not args.no_plots:
        plot_pca_3d(feat_df, out_dir / 'device_health_pca3d.png')
        plt.show()

    elapsed = time.monotonic() - t_start
    logger.info('Done. Total elapsed: %.1f s', elapsed)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
