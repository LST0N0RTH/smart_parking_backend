import os, ssl, time
from pathlib import Path
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

load_dotenv(Path(__file__).with_name(".env"))
host = os.getenv("MQTT_BROKER", "").strip()
port = int(os.getenv("MQTT_PORT", "443"))
path = os.getenv("MQTT_WEBSOCKET_PATH", "/mqtt").strip() or "/mqtt"
user = os.getenv("MQTT_USERNAME", "").strip()
print(f"target = wss://{host}:{port}{path}  user={user!r}")

result = {}
def on_connect(client, userdata, flags, reason_code, properties):
    result["rc"] = str(reason_code)

c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"coolkids-check-{os.getpid()}",
                transport="websockets", protocol=mqtt.MQTTv311)
c.ws_set_options(path=path)
c.tls_set_context(ssl.create_default_context())
c.username_pw_set(user, os.getenv("MQTT_PASSWORD", ""))
c.on_connect = on_connect
try:
    c.connect(host, port, keepalive=30)
except Exception as error:
    print("CONNECT ERROR:", repr(error))
    raise SystemExit(1)
c.loop_start(); time.sleep(5); c.disconnect(); c.loop_stop()
print("RESULT:", result.get("rc", "no CONNACK"))