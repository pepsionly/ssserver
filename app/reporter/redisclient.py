import json
from urllib.parse import urlparse, parse_qs
import redis
import config
from app.reporter import BaseReporter
from utils import CommonUtils

class RedisConn(BaseReporter):
    def __init__(self, **kwargs):
        conf = config.Config()
        self.redis_url = 'redis://:%s@%s:%s/0?client_name=python_dev' % \
                         (conf.REDIS_PASSWORD, conf.REDIS_HOST, conf.REDIS_PORT)
        self._conn = self.pool_from_url(**kwargs)

    def __del__(self):
        self._conn.close()

    def pool_from_url(self, **kwargs):
        url_options = dict()
        url = urlparse(self.redis_url)
        for name, value in iter(parse_qs(url.query).items()):
            url_options[name] = value[0]
        if 'db' not in url_options and url.path:
            try:
                url_options['db'] = int(url.path.replace('/', ''))
            except (AttributeError, ValueError):
                pass
        url_options['db'] = int(url_options.get('db', 0))
        # 这里我设置为True, 源码默认为false
        url_options['decode_responses'] = True
        _url = url.scheme + '://' + url.netloc
        url_options.update(kwargs)
        return redis.StrictRedis.from_url(_url, **url_options)

    def __getattr__(self, command):
        def _(*args, **kwds):
            return getattr(self._conn, command)(*args, **kwds)

        return _

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self._conn.close()
        else:
            return True

    def report_switch_online(self, brand, data_s, data_gw=''):
        if data_s:
            self._conn.set('switch_status:[%s]%s' % (brand, data_s['SID']), json.dumps(data_s))
        if data_gw:
            self._conn.set('gateway_status:[%s]%s' % (brand, data_gw['GID']), json.dumps(data_gw))

    def regular_report(self, gateway_id, switch_id, report_time, code, data):

        # 转化日期格式成'%Y-%m-%d %H:%M:%S'
        report_time = CommonUtils.standardize_datetime_210816144502(report_time)
        # 转化日期为时间戳
        time_stamp = CommonUtils.datetime_timestamp(report_time)
        payload = {
            'gateway_id': gateway_id,
            'switch_id': switch_id,
            'date': time_stamp,
            'code': code,
            'data   ': data,
        }
        self._conn.lpush('switch_data', json.dumps(payload))


    def get_switch_device_type_code(self, brand, sid):
        key = 'switch_status:[%s]%s' % (brand, sid)
        switch_status = self._conn.get(key)
        if not switch_status:
            return None
        switch_status = json.loads(switch_status)
        return switch_status.get('SDT')


    def get_gateway_device_type_code(self, brand, gid):
        key = 'gateway_status:[%s]%s' % (brand, gid)
        gateway_status = self._conn.get(key)
        if not gateway_status:
            return None
        gateway_status = json.loads(gateway_status)
        return gateway_status.get('GDT')

    def example1(self):
        """
        短连接示例
        """
        with RedisConn(db=1) as r:
            r.set('user:zhangshan', " zhangshan's description")
            result = r.get('user:zhangshan')
            print(result)

    def example2(self):
        """
        长连接示例
        """
        r = RedisConn(db=1, client_name='client23414')
        r.set('user:zhangshan', " zhangshan's description")
        result = r.get('user:zhangshan')
        print(result)
        r.close()
