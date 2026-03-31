import pandas as pd
import plotly.express as px

def main():
    # 1. Load the dataset
    # Ensure 'all_collision_ticks_W49ms.csv' is in your working directory
    df_ticks = pd.read_csv('./collision_exports/all_collision_ticks_W49ms.csv')
    df_ticks['tick_time'] = pd.to_datetime(df_ticks['tick_time'])
    df_ticks = df_ticks.sort_values(['source_file', 'device_uid', 'tick_time'])

    # 2. Calculate Inter-Arrival Times (Jitter)
    # Group by file+device to avoid differencing across separate sessions.
    # Also drop very large gaps between sessions (default 1s).
    max_inter_arrival_ms = 1000.0
    df_ticks['inter_arrival_ms'] = (
        df_ticks.groupby(['source_file', 'device_uid'])['tick_time']
        .diff()
        .dt.total_seconds() * 1000
    )
    df_clean = df_ticks.dropna(subset=['inter_arrival_ms'])
    df_clean = df_clean[df_clean['inter_arrival_ms'] <= max_inter_arrival_ms].copy()

    # 3. Define Status Groups
    failing_devices = ['CB100-2600577', 'CB100-2598385', 'CB100-2599429']
    at_risk_devices = ['CB100-2595836']
    new_devices = ['CB100-2597625', 'CB100-2598608', 'CB100-2599267']

    def get_status(uid):
        if uid in failing_devices:
            return 'Failing'
        elif uid in at_risk_devices:
            return 'At-Risk'
        elif uid in new_devices:
            return 'New'
        else:
            return 'Healthy'


    # 4. Calculate Advanced Features per Device
    features = []
    for device, group in df_clean.groupby('device_uid'):
        intervals = group['inter_arrival_ms']
        
        # Feature A: Timeout Ratio (Fraction of intervals pushed > 250ms)
        timeout_ratio = (intervals > 250).mean()
        
        # Feature B: Skewness (Measure of the "tail" caused by delays)
        skew = intervals.skew()
        
        # Feature C: Stability Index (Interquartile Range - tightness of the heartbeat)
        q75 = intervals.quantile(0.75)
        q25 = intervals.quantile(0.25)
        iqr = q75 - q25
        
        features.append({
            'Device ID': device,
            'Status': get_status(device),
            'Timeout Ratio': timeout_ratio,
            'Skewness': skew,
            'Stability Index (IQR)': iqr
        })

    df_features = pd.DataFrame(features)

    # 5. Create Interactive 3D Plot
    fig = px.scatter_3d(df_features, 
                        x='Timeout Ratio', 
                        y='Skewness', 
                        z='Stability Index (IQR)',
                        color='Status',
                        symbol='Status',
                        hover_name='Device ID',
                        color_discrete_map={'Failing': 'red', 'At-Risk': 'orange', 'Healthy': 'green', 'New': 'dodgerblue'},
                        symbol_map={'Failing': 'cross', 'At-Risk': 'diamond', 'Healthy': 'circle', 'New': 'square'},
                        title='3D Device Health Clustering (Interactive)')

    # Improve visual layout
    fig.update_traces(marker=dict(size=5))
    fig.update_layout(scene = dict(
                        xaxis_title='Timeout Ratio (>250ms)',
                        yaxis_title='Right-Tail Skew',
                        zaxis_title='Stability Index (IQR)'),
                        margin=dict(l=0, r=0, b=0, t=30))

    # 6. Show or Save
    fig.show()
    fig.write_html("device_health_3d.html") # Uncomment to save as file


if __name__ == "__main__":
    main()