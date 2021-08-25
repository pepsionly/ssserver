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

    def get_all(self, keys, mini_count=0):
        """
        批量查询某个key下的所有数据
        @param mini_count: 最少返回多少条
        @param keys: 模糊查询的key
        @return: 匹配的结果数组
        """
        keys = self._conn.keys(keys)
        if len(keys) < mini_count:
            return [], []
        return keys, self._conn.mget(keys)

    def del_all(self, keys):
        """
        @param keys: 模糊查询的key
        @return: 删除的数据条数
        """
        self._conn.delete(*self._conn.keys(keys))

    def hset(self, redis_model):
        """
        @param redis_model: RedisModels 实例
        @return:
        """
        # 断言
        assert isinstance(redis_model, RedisModel)
        for key, value in redis_model.obj.items():
            self._conn.hset(redis_model.key, key, value)

    def hget(self, redis_model, attr=''):
        """
        @param redis_model: RedisModels 实例
        @param attr: 需要取出的属性值
        @return:
        """
        # 断言
        assert isinstance(redis_model, RedisModel)
        if not attr:
            return self._conn.hgetall(redis_model.key)
        else:
            return self._conn.hget(redis_model.key, attr)

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
