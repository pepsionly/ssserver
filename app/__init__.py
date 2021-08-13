import sys

from flask import Flask
from config.dev import DevelopmentConfig
from app.mqttclient.mqttclient import MqttClient
from app.gatewayclient.hongfa import HongFa
from app.redisclient import RedisConn

if __name__ == '__main__':

    # 初始化服务实例
    app = Flask(__name__)
    # 初始化配置实例
    conf = DevelopmentConfig()
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
        return 'this is the gate way server'

    app.run()