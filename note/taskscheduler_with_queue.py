import json
import time
import threading
from application.gatewayclient.hongfa import HongFa


class TaskScheduler(object):
    """
         此为对网关/断路器请求的调度器，基于 redis list 实现对每个网关请求的队列，但测试存在问题，暂时不采用。
         主要问题如下：
            同时写宏发网关/断路器寄存器的请求存在请求不同，但是响应完全相同的情况：
                比如两个请求写多个相同的寄存器（写入的值不同），响应只会返回写成功的起始地址和寄存器个数

            故必须先判断请求是不是已经发出才开始监听响应，这样前端每次写多个寄存器就必须要经过以下过程：
            1.前端发出请求

            2.1 服务端响应：请求已经发出mqtt主题
            2.2 服务端响应：请求已入redis队列

            3.1 2.1的情况，前端就可以开始等待响应/或订阅redis获取结果

            3.2 2.2的情况则还需等服务端给出‘请求已经发出mqtt主题’的响应才能开始监听结果

            过程过于繁琐，故暂不采用

    """

    def __init__(self, mqtt_client, redis_conn):
        self.mqtt_client = mqtt_client
        self.redis_conn = redis_conn

    def queue_up(self, request_task):
        self.redis_conn.left_push(request_task)

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
