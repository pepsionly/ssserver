from flask import Flask, render_template
from gevent.pywsgi import WSGIServer
from gevent import monkey
import time

from application import MqttClient, RedisConn, HongFa, RequestScheduler
from config.dev import DevelopmentConfig
from config.prop import ProductionConfig
from data.mapper import Mapper

monkey.patch_all()
app = Flask(__name__)

@app.route('/')
def connect():
	return "connected test"

@app.route('/index')
def index_test():
	time.sleep(15)
	return 'waited for 15s'

if __name__ == "__main__":
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

	# 初始化请求任务调度器
	scheduler = RequestScheduler(mqtt_client, redis_conn)

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

	server = WSGIServer(("0.0.0.0", 8089), app)
	print("Server started")
	server.serve_forever()