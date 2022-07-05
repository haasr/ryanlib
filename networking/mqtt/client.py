import paho.mqtt.client as paho
from cryptography.fernet import Fernet
from paho import mqtt
import sys

class MQTTClient():
    def __init__(self, auto_reconnect=False, client_id='', userdata=None, protocol=paho.MQTTv5,
                    tls_version=mqtt.client.ssl.PROTOCOL_TLS, thread_name=''):
        try:
            self.client = paho.Client(client_id=client_id,
                                        userdata=userdata, protocol=protocol)
        except Exception as e:
            print(e)
            sys.exit(1)

        self.set_tls_version(tls_version)
        self.running = False

        if auto_reconnect:
            self.client.on_disconnect = self.on_disconnect

    def set_tls_version(self, version):
        try:
            self.client.tls_set(tls_version=version)
            return True
        except Exception as e:
            print(e)
            return False

    def set_credentials(self, username, password):
        try:
            self.client.username_pw_set(username, password)
            return True
        except Exception as e:
            print(e)
            return False

    def connect(self, host, port=8883):
        try:
            self.client.connect(host=host, port=port)
            self.last_host = host
            self.last_port = port
            return True
        except Exception as e:
            print(e)
            return False

    def subscribe(self, topic, qos):
        try:
            self.client.subscribe(topic, qos=qos)
            return True
        except Exception as e:
            print(e)
            return False

    def subscribe_map(self, topics_qos_map):
        for k,v in topics_qos_map.items():
            self.subscribe(k, v)

    def start(self):
        if not self.running:
            self.client.loop_start()
            self.running = True

    def stop(self):
        if self.running:
            self.client.loop_stop()
            self.running = False

    def publish(self, topic, payload):
        self.client.publish(topic, payload)

    def on_disconnect(self, client, userdata, rc=0):
        self.stop()
        self.connect(self.last_host, self.last_port)
        self.start()


class FernetMQTTClient:
    def __init__(self, client, key, encoding='utf-8'):
        self.client = client
        self.ENCODING = encoding
        self.init_cipher(key)
        self.client.client.on_message=self.on_message
        self.client.start()

    def init_cipher(self, key):
        self.cipher = Fernet(key)

    def on_message(self, client, userdata, msg):
        payload = self.cipher.decrypt(msg.payload)
        payload = payload.decode(self.ENCODING)
        s = "\n" + msg.topic + f"(QoS={msg.qos})"
        s += "\n" + payload
        return s

    def publish(self, topic, message):
        enc_buffer = self.cipher.encrypt(bytes(message, self.ENCODING))
        self.client.publish(topic, enc_buffer)
        return f"[ PUB ] {message}"

    def stop(self):
        self.client.stop()

"""
Example use:

from networking.mqtt.client import MQTTClient, FernetMQTTClient
from datetime import datetime as dt
from dotenv import load_dotenv
from os import getenv

load_dotenv()

base_client = MQTTClient(auto_reconnect=True)
base_client.set_credentials(getenv('MY_MQTT_USERNAME'),
                            getenv('MY_MQTT_PASSWORD'))
base_client.connect(getenv('MY_MQTT_URL'))

# Subscribe with QOS specified:
base_client.subscribe_map({ "bot/client/info": 0, "bot/client/res": 1 }) # Subscribing to client info and client response channels


# To use Fernet encryption, simply generate a key:

# >>> from cryptography.fernet import Fernet
# >>> key = Fernet.generate_key()
# >>> key
# '0S9Jha55CI4kdSUZByD2TRMUHv8Y-HXVRy9x8dCONgQ='

# Then store the key as an environment variable whereever you use a Fernet MQTT client and just init the client
# with that key (must be the same key for all Fernet clients that talk to each other):

fernet_client = FernetMQTTClient(base_client, getenv('MY_MQTT_FERNET_TOKEN'))

# Posting to server info channel:
fernet_client.publish("bot/server/info", "STATUS: ONLINE")

PUB_TOPIC = 'bot/server/req'
last_message_id = ''

# Functions to publish and ignore duplicates (when integrated in my Django app
# I noticed that sometimes two identical pub requests got generated from a POST request)

def get_message_id():
    return str(dt.now().isoformat())

def publish(message, message_id):
    global last_message_id
    if message_id != last_message_id:
        fernet_client.publish(PUB_TOPIC, message)
        last_message_id = message_id

publish('This is a test from the server end', get_message_id())

"""