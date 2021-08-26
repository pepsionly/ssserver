from flask import Flask
from data.mapper import Mapper
from application.redisclient import RedisConn
from config.dev import DevelopmentConfig
from application.mqttclient.mqttclient import MqttClient
from application.gatewayclient.hongfa import HongFa
from application.taskscheduler import TaskScheduler
from config.const import Const

if __name__ == '__main__':
    """
        初始化启动mqtt客户端
    """
    # 初始化配置实例
    conf = DevelopmentConfig()
    # conf = ProductionConfig()

    # 初始化/连接mqtt客户端
    mqtt_client = MqttClient(conf)
    mqtt_client.connect_mqtt()

    # 初始化消息处理器并绑定到mqtt客户端
    mapper = Mapper()
    redis_conn = RedisConn(db=0, client_name='python_dev')
    hongfa_client = HongFa(conf, redis_conn, mapper)
    mqtt_client.bind_gateway_client(hongfa_client)

    # 启动mqtt客户端开始监听
    mqtt_client.run()

    # 初始化并启动任务调度器
    scheduler = TaskScheduler(mqtt_client, redis_conn)
    scheduler.run()

    """
        初始化、启动服务
    """
    # 初始化web服务实例
    app = Flask(__name__)
    # 加载配置
    app.config.from_object(conf)

    # 注册接口蓝图和全局变量
    from application.hongfa_apis import hongfa

    app.register_blueprint(hongfa, url_prefix='/hongfa/')

    # 注册全局变量
    app.__setattr__('hongfa', hongfa_client)
    app.__setattr__('mqtt_client', mqtt_client)
    app.__setattr__('conf', conf)
    app.__setattr__('mapper', mapper)
    app.__setattr__('redis_conn', redis_conn)
    app.__setattr__('scheduler', scheduler)

    # 测试用的方法
    @app.route('/')
    def test_func():
        return 'testing'

    # 启动服务端
    app.run('127.0.0.1', 5000)
