import abc
from threading import Thread
import config

class MqttClient(abc.ABC):

    """
        mqtt客户端的抽象类，子类必须实现
    """
    def __init__(self, conf):
        assert isinstance(conf, config.Config)
        self.client = None
        self.conf = conf
        Thread.__init__(self)

    @abc.abstractmethod
    def subscribe(self):
        pass

    @abc.abstractmethod
    def run(self):
        pass

    """
    @abc.abstractmethod
    def parse_ids(self):
        pass

    @abc.abstractmethod
    def check_crc(self):
        pass

    @abc.abstractmethod
    def parse_ori_data(self):
        pass
    
    @abc.abstractmethod
    def publish(self):
        pass
    """


