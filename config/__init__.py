
class Config(object):
    """项目配置核心类"""
    # 调试模式
    DEBUG = False

    # 配置日志
    # LOG_LEVEL = "DEBUG"
    LOG_LEVEL = "INFO"

    SECRET = 'my secret key'
    TEMPLATES_AUTO_RELOAD = True

    # 配置redis
    REDIS_HOST = '192.168.30.11'
    # REDIS_HOST = '192.168.0.218'
    REDIS_PORT = '6379'
    REDIS_PASSWORD = 'xyz123456'
    # REDIS_PASSWORD = 'Pyth.2021'
    REDIS_POLL = 16

    # MQTT配置
    MQTT_BROKER_URL = 'iot.htemp.net'
    MQTT_BROKER_PORT = 1883
    MQTT_USERNAME = 'python_dev'
    MQTT_PASSWORD = '123456@'
    MQTT_KEEPALIVE = 60
    MQTT_TLS_ENABLED = False
    MQTT_LAST_WILL_TOPIC = 'home/lastwill'
    MQTT_LAST_WILL_MESSAGE = 'bye'
    MQTT_LAST_WILL_QOS = 2

    # 其他
    hongfa_subscribe = ["hongfa/+/upload/", 'hongfa/+/will/']
    hongfa_publish = "hongfa/+/download/"
    client_id = 'python_client_test'
    sqllite_file = '../data/sqlite.db'


class DevConfig(Config):
    broker_username = 'python_dev'
    broker_password = '123456@'

