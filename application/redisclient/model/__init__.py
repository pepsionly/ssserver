import abc
import json
from json import JSONDecodeError

import redis


class RedisColumn:

    def __init__(self, no_null=False):
        self.no_null = no_null


class RedisModel(abc.ABC):
    obj = {}

    def __init__(self, obj):
        """
        @param obj:
        """
        self.obj = obj
        assert (self.validate_obj(obj))

    def validate_obj(self, obj):
        obj_attrs = dir(self)
        for attr in obj_attrs:
            if isinstance(eval('self.%s' % attr), RedisColumn) and eval('self.%s' % attr).no_null and not obj.get(attr):
                return False
        return True

    @property
    @abc.abstractmethod
    def key(self):
        """
        @return: redis 键
        """

    @property
    def json(self):
        """
        @return:  模型对象对应的json字符串
        """
        return json.dumps(self.obj)

    def load(self, obj_json):
        """
        @return:  模型对象对应的json字符串
        """
        try:
            obj = json.loads(obj_json)
        except (JSONDecodeError, TypeError):
            obj = None

        if not obj:
            return None
        assert (self.validate_obj(obj))
        return obj
