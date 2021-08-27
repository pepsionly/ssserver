import json
from utils.exceptions import RequestNotResponded
from application.gatewayclient.hongfa import HongFa

class RequestScheduler(object):

    def __init__(self, mqtt_client, redis_conn):
        self.mqtt_client = mqtt_client
        self.redis_conn = redis_conn

    def is_lock(self, topic):
        key = 'locks:' + topic
        lock = self.redis_conn.get(key)
        if lock:
            return True
        else:
            return False

    def assign_task(self, topic, payload, qos=0):
        locked = self.is_lock(topic)
        if locked:
            raise RequestNotResponded
        else:
            self.lock_gw(topic, payload)
            self.mqtt_client.publish(topic, payload, qos)
        return HongFa.parse_request_key(topic, payload)

    def lock_gw(self, topic, task_str, ex=30):
        key = 'locks:' + topic
        # 设置锁过期的时间
        if any(i in task_str for i in ['GWD_SLAVE_ALL', 'ReportAll', 'FindAllSalves', 'SyncTime']):
            ex = 5
        self.redis_conn.set(key, '1', ex=ex)
