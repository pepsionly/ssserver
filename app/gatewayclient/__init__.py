import abc
import config
class GatewayClient(abc.ABC):
    """
        网关客户端的抽象类
    """
    def __init__(self, conf):
        """
        读取配置，初始化
        :param conf: 配置对象，必须继承config.Config
        """
        assert isinstance(conf, config.Config)
        self.conf = conf
        self.subscribe_topics = []
        self.topic_handlers = []

    @abc.abstractmethod
    def set_subscribe_topics(self):
        """
        重写这个方法，设置本GatewayClient订阅的主题列表, 既subscribe_topics属性
        """

    @abc.abstractmethod
    def set_topic_handlers(self):
        """
        重写这个方法，给出主题与消息handler函数名的映射, 既topic_handlers属性，与和subscribe_topics属性一一：
        subscribe_topics = ['topic1', 'topic2', ;'topic3', 'topic4']
        topic_handlers = ['handle_topic_topic1', None, ;'handle_topic_topic3', None]
        未实现：若topic_handlers未设置，则程序自动检测所有handle_topic_*方法，并按顺序映射
        """
        pass
