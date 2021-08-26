import json
import time
from application.redisclient.model.models import RequestTask
import threading
from application.gatewayclient.hongfa import HongFa

class TaskScheduler(object):

    def __init__(self, mqtt_client, redis_conn):
        self.mqtt_client = mqtt_client
        self.redis_conn = redis_conn

    def queue_up(self, request_task):
        self.redis_conn.left_push(request_task)
        topic = request_task.obj['topic']
        lock = self.redis_conn.get('temp:topics:' + topic)

    def find_unlock_topics(self):
        all_queues = self.redis_conn.keys('queues:*')
        all_locks = self.redis_conn.keys('locks:*')
        topic_all_queues = [item[7:] for item in all_queues] if all_queues else []
        topic_all_locks = [item[6:] for item in all_locks] if all_locks else []
        return list(set(topic_all_queues).difference(set(topic_all_locks)))

    def get_tasks(self, unlock_topics):
        request_keys = ['queues:' + item for item in unlock_topics]
        p = self.redis_conn.pipeline()
        for request_key in request_keys:
            p.rpop(request_key)
        data = p.execute()
        return data if data else []

    def assign_tasks(self, request_tasks):
        for request_task in request_tasks:
            task_obj = json.loads(request_task)
            self.lock_gw(task_obj['topic'], request_task)
            key, fc = HongFa.parse_request_key(task_obj['topic'], task_obj['payload'])
            if key:
                self.redis_conn.set(key, 'waiting', ex=60)
            self.mqtt_client.publish(task_obj['topic'], task_obj['payload'], task_obj['qos'])

    def lock_gw(self, topic, task_str, ex=60):
        key = 'locks:' + topic
        # 设置锁过期的时间
        if any(i in task_str for i in ['GWD_SLAVE_ALL', 'ReportAll', 'FindAllSalves', 'SyncTime']):
            ex = 5
        self.redis_conn.set(key, '1', ex=ex)

    def _run(self):
        while True:
            unlock_topics = self.find_unlock_topics()
            request_tasks = self.get_tasks(unlock_topics)
            self.assign_tasks(request_tasks)
            time.sleep(0.1)

    def run(self):
        t1 = threading.Thread(target=self._run, name='scheduler')
        t1.start()
