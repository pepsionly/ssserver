from urllib.parse import urlparse, parse_qs
import redis
import config
from .model import RedisModel

class RedisConn(object):
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

    def get_all(self, key, mini_count=2):
        """
        批量查询某个key下的所有数据
        @param mini_count:
        @param key:
        @return:
        """
        keys = self._conn.keys(key + ':*')
        if len(keys) < mini_count:
            return []
        return self._conn.mget(keys)

    def set_one(self, redis_model):
        assert isinstance(redis_model, RedisModel)
        key = redis_model.key
        payload = redis_model.json
        return self._conn.set(key, payload)

    def get_one(self, redis_model):
        assert isinstance(redis_model, RedisModel)
        key = redis_model.key
        data = self._conn.get(key)
        return redis_model.load(data)

    def left_push(self, redis_model):
        assert isinstance(redis_model, RedisModel)
        key = redis_model.key
        payload = redis_model.json
        return self._conn.lpush(key, payload)

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
