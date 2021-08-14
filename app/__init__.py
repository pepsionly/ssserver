import sys

from flask import Flask
from config.dev import DevelopmentConfig
from config.prop import ProductionConfig
from app.mqttclient.mqttclient import MqttClient
from app.gatewayclient.hongfa import HongFa
from app.redisclient import RedisConn

if __name__ == '__main__':

    # 初始化服务实例
    app = Flask(__name__)
    # 初始化配置实例
    conf = DevelopmentConfig()
    # conf = ProductionConfig()
    # 加载配置
    app.config.from_object(conf)

    # 初始化mqtt客户端
    mqtt_client = MqttClient(conf)
    hongfa_client = HongFa(conf)

    # 连接mqtt服务器
    mqtt_client.connect_mqtt()

    # 绑定消息处理器
    mqtt_client.bind_gateway_client(hongfa_client)
    mqtt_client.run()

    @app.route('/')
    def index():
        topic = 'hongfa/FFD1212006105728/upload/'
        payload = '{"GWD_RAW_04":"D3F60000001100040E001700020002FFD1212006105728","SD_RAW_04":"63FD000D000F00040C000500026B02210710091756"}'
        payload = '{"GWD_RAW_04":"D3F60000001100040E001700020002FFD1212006105728","SD_RAW_04":"FE290007000F00040C000300014C36210605133128"}'
        qos = 0
        mqtt_client.publish(topic, payload, qos)

        return 'this is the gate way server'

    app.run()