import websocket
import json
import os
from polygon import RESTClient
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('POLYGON_API_KEY')
client = RESTClient(API_KEY)
SOCKET_URL = 'wss://delayed.polygon.io/stocks'  # 15-min delayed stream

def on_open(ws):
    print("Opened connection")
    auth_data = {"action": "auth", "params": API_KEY}
    ws.send(json.dumps(auth_data))

    # Subscribe to aggregate (per-minute bars) for AAPL
    sub_data = {"action": "subscribe", "params": "A.AAPL"}
    ws.send(json.dumps(sub_data))

def on_message(ws, message):
    print("Received message:", message)

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("Closed connection")

ws = websocket.WebSocketApp(
    SOCKET_URL,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.run_forever()
