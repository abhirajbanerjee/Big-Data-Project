import os
import upstox_client
from upstox_client.rest import ApiException
from datetime import datetime
from kafka import KafkaProducer
import json
import time

configuration = upstox_client.Configuration()
configuration.access_token = os.environ.get("UPSTOX_ACCESS_TOKEN")

api_instance = upstox_client.MarketDataApi(upstox_client.ApiClient(configuration))

kafka_bs = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
producer = KafkaProducer(
    bootstrap_servers=kafka_bs,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_and_send():
    try:
        response = api_instance.get_market_quote(
            instrument_key=["NSE_EQ|INE669E01016"],
            interval="1minute"
        )

        tick = response.data[0]
        # adapt fields to what the upstox client returns
        message = {
            "timestamp": datetime.utcnow().isoformat(),
            "ltp": getattr(tick, "ltp", None),
            "volume": getattr(tick, "volume", None),
            "ohlc": getattr(tick, "ohlc", {}).__dict__ if getattr(tick, "ohlc", None) else {}
        }

        producer.send("upstox_ticks", message)
        print("Sent:", message)

    except ApiException as e:
        print("Exception when calling Upstox API:", e)
    except Exception as e:
        print("Unexpected error:", e)

def main():
    while True:
        fetch_and_send()
        time.sleep(1)

if __name__ == "__main__":
    main()
