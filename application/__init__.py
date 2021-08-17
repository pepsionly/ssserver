from flask import Flask

from application.redisclient import RedisConn
from config.dev import DevelopmentConfig
from application.mqttclient.mqttclient import MqttClient
from application.gatewayclient.hongfa import HongFa

if __name__ == '__main__':

    # 初始化服务实例
    app = Flask(__name__)
    # 初始化配置实例
    conf = DevelopmentConfig()
    # conf = ProductionConfig()
    # 加载配置
    app.config.from_object(conf)


    # 初始化/连接mqtt客户端
    mqtt_client = MqttClient(conf)
    mqtt_client.connect_mqtt()

    # 绑定消息处理器
    reporter = RedisConn(db=0, client_name='python_dev')
    hongfa_client = HongFa(conf, reporter)
    mqtt_client.bind_gateway_client(hongfa_client)

    # 启动mqtt客户端
    mqtt_client.run()

    @app.route('/')
    def index():
        topic = 'hongfa/FFD1212006105728/upload/'
        payload = '{"GWD_RAW_04":"D3F60000001100040E001700020002FFD1212006105728","SD_RAW_04":"63FD000D000F00040C000500026B02210710091756"}'
        # payload = '{"GWD_RAW_04":"D3F60000001100040E001700020002FFD1212006105728","SD_RAW_04":"FE290007000F00040C000300014C36210605133128"}'
        qos = 0
        mqtt_client.publish(topic, payload, qos)
        return 'this is the gate way server'

    app.run()