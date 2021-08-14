
from paho.mqtt import client as mqtt_client
from . import MqttClient
from utils.__init__ import *
from utils.log import Log
from .. import gatewayclient
from config.dev import DevelopmentConfig
logger = Log(__name__).getlog()


class MqttClient(MqttClient):

    def __init__(self, conf):
        super().__init__(conf)

    def __del__(self):
        self.client.disconnect()

    def on_message(self, mosq, obj, msg):
        # This callback will be called for messages that we receive that do not
        # match any patterns defined in topic specific callbacks, i.e. in this case
        # those messages that do not have topics $SYS/broker/messages/# nor
        # $SYS/broker/bytes/#
        print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))


    def event_received(self, client, userdata, msg):
        """data = message.payload.decode("utf-8")
        logging.info("received url %r %r", message.topic, str(data))"""
        # print(f"unhandled `{msg.payload.decode()}` from `{msg.topic}` topic")
        logger.warning(f"unhandled `{msg.payload.decode()}` from `{msg.topic}` topic")

    def event_publish(self, client, userdata, result):
        pass


    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            logging.warning("Unexpected client disconnect for %r, will reconnect")

    def connect_mqtt(self):
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)
        client = mqtt_client.Client(self.conf.client_id)
        client.on_connect = on_connect
        # 生产配置抑制mqtt回调异常
        client.suppress_exceptions = False if isinstance(self.conf, DevelopmentConfig) else True
        client.username_pw_set(self.conf.MQTT_USERNAME, password=self.conf.MQTT_PASSWORD)
        client.on_message = self.event_received
        client.on_publish = self.event_publish
        client.on_disconnect = self.on_disconnect
        client.connect(self.conf.MQTT_BROKER_URL, self.conf.MQTT_BROKER_PORT)
        self.client = client

    def subscribe(self, topic):
        self.client.subscribe(topic)

    def add_message_handler(self, topic, handler):
        self.client.message_callback_add(topic, handler)

    def bind_gateway_client(self, gt_client):
        assert isinstance(gt_client, gatewayclient.GatewayClient)
        topics = gt_client.subscribe_topics
        hanlders = gt_client.topic_handlers

        for i in range(len(topics)):
            self.subscribe(topics[i])
            print(topics[i])
            if hanlders[i]:
                self.add_message_handler(topics[i], hanlders[i])

    def disconnect(self):
        self.client.loop_stop()

    def publish(self, topic, payload, qos):
        self.client.publish(topic, payload, qos)

    def run(self):
        self.client.loop_start()