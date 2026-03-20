
# Technical Report: Thermal Drift Analysis of CB100 Devices

### 1. Executive Summary

This report analyzes the reporting frequency of "CB100" data loggers to determine if thermal conditions influence device performance. The study utilized a custom Python-based ETL (Extract, Transform, Load) pipeline to parse batch logs and correlate timestamped readings with ambient temperature data.

**Key Findings:**

* **Thermal Dependency Confirmed:** A statistically significant slowdown in reporting frequency was observed across almost all devices as ambient temperature increased.
* **Magnitude of Drift:** The devices exhibit a systematic drift of approximately **1 millisecond per 5°C rise**, resulting in a ~0.5% slowdown over a 20°C range.
* **Root Cause:** The behavior is consistent with the thermal drift characteristics of a microcontroller's internal RC oscillator.

---

### 2. Methodology

The analysis focused on determining whether the reporting interval () changes as a function of temperature.

#### 2.1 Statistical Approach

To ensure robustness against outliers common in sensor logs (e.g., connection dropouts), the analysis employed non-parametric testing rather than standard means-based testing (t-tests).

* **Metric:** The **Median** inter-sample time () was used to represent "typical" reporting speed, effectively ignoring extreme outliers caused by device downtime.
* **Permutation Test:** A custom permutation test (2,000 permutations) was implemented to assess the difference in medians between adjacent temperature bins (e.g., 20°C vs. 25°C). This calculates the probability that the observed slowdown occurred by random chance.
* **Multiple Testing Correction:** To control for false positives across multiple devices and temperature bins, the **Benjamini-Hochberg (BH)** procedure was applied to control the False Discovery Rate (FDR) at .

---

### 3. Results Analysis

#### 3.1 Observed Trend

The data reveals a consistent positive correlation between temperature and reporting interval ().

* **Cold Operation (20–25°C):** Devices typically report every **~202–203ms** (~4.95 Hz).
* **Hot Operation (35–40°C):** Reporting intervals extend to **~207–208ms** (~4.80 Hz).
* **Rate of Change:** The drift is approximately linear, averaging **+1ms for every +5°C**.

#### 3.2 Statistical Significance

The results strongly reject the null hypothesis (that temperature has no effect on timing).

* **P-Values:** The permutation tests frequently returned the minimum possible p-value for the simulation depth (), indicating the observed difference was more extreme than 100% of the 2,000 randomized trials.
* **Significance:** The `significant` flag (derived from BH-adjusted q-values) remained **TRUE** for the vast majority of temperature steps, confirming the effect is systematic and not random noise.

| Temp Range | Median  (Example Device) | Change | Significance |
| --- | --- | --- | --- |
| 20°C – 25°C | 203ms | - | - |
| 25°C – 30°C | 204ms | +1ms | **Significant** |
| 35°C – 40°C | 208ms | +4ms (cumulative) | **Significant** |

---

### 4. Root Cause Analysis

The observed magnitude of drift (~0.5% over 20°C) points to hardware-level clock variance rather than firmware logic errors.

#### 4.1 Primary Cause: Internal RC Oscillator Drift

Microcontrollers in low-power IoT devices often utilize an internal Resistor-Capacitor (RC) oscillator instead of a high-precision Quartz crystal.

* **Mechanism:** As temperature rises, the electrical resistance within the oscillator circuit increases. This lengthens the charge/discharge cycle of the capacitor, effectively "stretching" the duration of a single clock tick.
* **Impact:** A firmware command to `sleep(200ms)` counts a fixed number of clock ticks. If the ticks themselves are longer, the physical sleep duration extends beyond 200ms. The observed **0.5% drift** aligns with standard specifications for internal RC oscillators (typically 1–5% over full temp range).

### 5. Short term mediation

While a <1% timing variance is acceptable for general logging, it may impact applications requiring high-precision temporal correlation.

1. **Timestamp Reliability:** Do not rely on the device-generated timestamp for precise synchronization. Use the server-side receipt timestamp (`captured_at`) where possible.






### 6. Firmware Update
3. **Future Hardware:** For future hardware revisions requiring strictly stable timing, specify an **external Quartz crystal oscillator (XTAL)** to eliminate thermal clock drift.