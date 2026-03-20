Technical Investigation into Thermal Clock Drift in CB100 Series Devices

Date: January 9, 2026
Device Model: CB100 (Hardware: STM32WB5MMG)
Subject: Root Cause Analysis of Timestamp Drifting and Reporting Latency

1. Executive Summary

This report details the findings of an investigation into reporting frequency inconsistencies observed in CB100 data loggers. By correlating batch logs with ambient temperature data, a systematic performance variance was isolated.

Key Findings:

Thermal Dependency: A statistically significant slowdown in device reporting frequency was confirmed. The device clock slows by approximately 0.5% over a 20°C rise (approx. 1ms delay per 5°C).

Root Cause: The drift characteristics match the profile of an Internal RC Oscillator. Analysis of the STM32WB5MMG hardware indicates that while a high-precision LSE crystal is physically present in the package, the firmware is likely defaulting to the lower-accuracy internal LSI clock.

Power Impact: Correcting this issue by enabling the crystal oscillator will incur a negligible current penalty (~300 nA) but will likely result in net-positive system energy savings by reducing radio guard bands.

Recommendation: Update firmware configuration to select the LSE (Low-Speed External) crystal as the clock source for the Real-Time Clock (RTC) and Low-Power Timer.

2. Methodology

The analysis utilized a custom Python-based ETL pipeline (eda.py) to parse batch logs, extract reading timestamps, and correlate them with asynchronous temperature logs using linear interpolation.

2.1 Statistical Approach

To ensure robustness against common sensor connection dropouts, non-parametric testing was employed:

Metric: The Median inter-sample time ($\Delta t$) was used to represent "typical" reporting speed.

Test: A Permutation Test for Difference in Medians (2,000 permutations) assessed the probability that timing differences between temperature bins were random.

Correction: The Benjamini-Hochberg (BH) procedure was applied to control the False Discovery Rate (FDR) at $\alpha = 0.05$.

3. Data Analysis & Findings

3.1 Observed Trend

There is a consistent, positive linear correlation between ambient temperature and the reporting interval ($\Delta t$).

Operational State

Temp Range

Reporting Interval (Median)

Frequency

Cold

20°C – 25°C

~202–203 ms

~4.95 Hz

Hot

35°C – 40°C

~207–208 ms

~4.80 Hz

Rate of Change: The drift averages +1ms for every +5°C increase.

3.2 Statistical Significance

The null hypothesis (that temperature has no effect on timing) was rejected with high confidence.

P-Values: Permutation tests frequently returned $p \approx 0.0005$, indicating the observed slowdown was more extreme than 100% of randomized simulations.

Significance: The BH-adjusted q-values confirmed the effect is systematic across the device fleet and not attributable to random noise.

4. Root Cause Analysis

The magnitude of the drift (~5000 ppm) is the primary indicator of the root cause.

4.1 The Hardware Reality (STM32WB5MMG)

The device is built on the STM32WB5MMG System-in-Package (SiP). Crucially, this module integrates the timing crystals inside the package. It contains:

LSE Crystal: 32.768 kHz precision crystal.

HSE Crystal: 32 MHz RF precision crystal.

4.2 The Diagnosis: Firmware Configuration Error

Despite the presence of a precision crystal, the data shows the signature of an Internal RC Oscillator.

LSI (Internal Low-Speed Oscillator): The datasheet specifies an accuracy of ±5% over the temperature range. The observed 0.5% drift falls squarely within the operational characteristics of this clock.

Conclusion: The firmware RCC_BDCR (Backup Domain Control Register) is likely configured to use the default LSI source rather than the available LSE crystal.

5. Impact Assessment

5.1 Battery Life Implications

Moving from the drifting LSI (RC) to the precise LSE (Crystal) involves a trade-off:

Current Draw: The LSE crystal consumes ~300 nA more than the internal LSI. On a standard coin cell, this impact is negligible (months of shelf life over a decade).

System Savings: The current RC drift necessitates a wide "Guard Band" for radio communication windows. By switching to the precise LSE, the radio stack can wake up more accurately, reducing the "listening" time of the high-power (5–10 mA) radio.

Net Result: Switching to LSE is expected to be power-neutral or power-positive.

Warning: Do not use the HSE (32 MHz) crystal for the sleep timer. Keeping the HSE active prevents the MCU from entering deep sleep modes, which would drain the battery rapidly.

5.2 Latency and Network Jitter

Clock drift creates a linear ramp in End-to-End Lag.

Drift Slope: As the device heats up, the lag ($T_{Gateway} - T_{Device}$) increases because the device seconds become "longer" than real seconds.

Jitter: The noise variance in the lag plot represents network/OS latency. Vertical spikes indicate packet retries.

6. Recommendations

Firmware Update (Primary Fix):

Configure the clock tree to enable the LSE Oscillator.

Set the RTC Clock Source to LSE.

Ensure LSE Drive Capability is set to "Low" (default for the SiP module) to optimize power.

Data Handling (Interim Fix):

Until firmware is updated, do not rely on device-generated timestamps for synchronization. Use the server-side captured_at timestamp.

If necessary, apply a linear correction: $T_{corr} = T_{raw} - (0.2 \text{ms}/^\circ\text{C} \times \Delta\text{Temp})$.

Appendices

Appendix A: Statistical Methodology Details

Permutation Test for Medians:

Observed Difference: Calculate $Median(T_{hot}) - Median(T_{cold})$.

Shuffle: Combine all readings, shuffle randomly, and split into two groups of original size.

Simulate: Calculate the median difference of the shuffled groups. Repeat 2,000 times.

P-Value: The fraction of simulations where the random difference $\ge$ observed difference.

Benjamini-Hochberg Correction:
To prevent false positives when testing multiple temperature bins, p-values are ranked. The critical value is adjusted based on rank: $P_{critical} = \frac{Rank}{TotalTests} \times \alpha$.

Appendix B: Clock Source Comparison

Clock Source

Type

Drift Spec

Battery Impact

Suitability

LSI

Internal RC

±50,000 ppm (5%)

~120 nA

Current (Poor)

LSE

External Crystal

±20 ppm

~450 nA

Recommended

HSE

External Crystal

±20 ppm

~1,000,000 nA

Forbidden for Sleep

Appendix C: Latency Assessment Formulas

Passive Lag (Drift Monitoring):


$$\text{Lag} = T_{\text{Gateway Log}} - T_{\text{Device TS}}$$

Interpretation: Slope = Thermal Drift. Fuzziness = Jitter.

Active Round-Trip Time (True Latency):
Requires firmware support for a "Ping" payload.


$$\text{Network Latency} \approx \frac{T_{\text{Gateway Receipt}} - T_{\text{Gateway Send}}}{2}$$