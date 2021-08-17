import abc

class BaseReporter(abc.ABC):
    """
        上报器基类
    """
    @abc.abstractmethod
    def report_switch_online(self, brand, data_switch, data_gateway=''):
        """
            汇报断路器在线
        """
        pass

    @abc.abstractmethod
    def regular_report(self, gateway_id, switch_id, report_time, code, data):
        """
            定期上报的断路器数据
        """
        pass

    @abc.abstractmethod
    def get_switch_device_type_code(self, brand, device_id):
        """
            查询断路器设备类型代码
        """

    @abc.abstractmethod
    def get_gateway_device_type_code(self, brand, sid):
        """
            查询断路器设备类型代码
        """
