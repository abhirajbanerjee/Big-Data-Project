import os
from kafka import KafkaConsumer
import json
import threading
import time
import dash
from dash import dcc, html
import plotly.graph_objs as go
from dash.dependencies import Input, Output

kafka_bs = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumer = KafkaConsumer(
    "upstox_orderflow",
    bootstrap_servers=kafka_bs,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
)

data_buffer = []

def poll_kafka():
    for msg in consumer:
        try:
            data_buffer.append(msg.value)
            if len(data_buffer) > 200:
                data_buffer.pop(0)
        except Exception as e:
            print("Error parsing message:", e)

kafka_thread = threading.Thread(target=poll_kafka)
kafka_thread.daemon = True
kafka_thread.start()

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H2("Orderflow Dashboard"),
    dcc.Graph(id="graph"),
    dcc.Interval(id="interval", interval=1000, n_intervals=0)
])

@app.callback(
    Output("graph", "figure"),
    [Input("interval", "n_intervals")]
)
def update_graph(n):
    if not data_buffer:
        return go.Figure()

    # adapt keys according to data_processor output
    timestamps = [d.get("window_start") for d in data_buffer]
    volumes = [d.get("total_volume", 0) for d in data_buffer]
    ltps = [d.get("sum_ltp", 0) for d in data_buffer]

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=timestamps, y=volumes, mode="lines", name="Volume"))
    fig.add_trace(go.Scatter(x=timestamps, y=ltps, mode="lines", name="Sum LTP"))

    return fig

if __name__ == "__main__":
    try:
        app.run_server(host="0.0.0.0", port=8050)
    finally:
        print("✓ Dashboard stopped")
