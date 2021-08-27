from config.const import Const

consts = Const()


class BaseCustomException(Exception):

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = {'code': self.status_code, 'message': self.message}
        if self.payload:
            rv['payload'] = self.payload
        return rv


class DeviceTypeNotFound(BaseCustomException):
    """
        查询sqlite映射表未找到对应设备类型是抛出此异常，flask框架统一处理
    """

    def __init__(self, message='device type not found', status_code=consts.INVALID_DEVICE_TYPE, payload=None):
        BaseCustomException.__init__(self, message, status_code, payload)


class InvalidParamID(BaseCustomException):
    """
        请求的参数ID无效时抛出此异常，flask框架统一处理
    """

    def __init__(self, message='invalid param id', status_code=consts.INVALID_PARAM_ID, payload=None):
        BaseCustomException.__init__(self, message, status_code, payload)


class InvalidParamValue(BaseCustomException):
    """
        请求的参数值无效时抛出此异常，flask框架统一处理
    """

    def __init__(self, message='invalid param value', status_code=consts.INVALID_PARAM_VALUE, payload=None):
        BaseCustomException.__init__(self, message, status_code, payload)


class RequestNotResponded(BaseCustomException):
    """
        在处理请求时，若网关尚有正在处理的任务时抛出此异常，flask框架统一处理
    """

    def __init__(self, message='last request is still not responded', status_code=consts.Request_Unhandled,
                 payload=None):
        BaseCustomException.__init__(self, message, status_code, payload)


class ResponseTimeOut(BaseCustomException):
    """
        网关/断路器响应超时
    """

    def __init__(self, message='gateway/switch respond timeout', status_code=consts.Request_Unhandled,
                 payload=None):
        BaseCustomException.__init__(self, message, status_code, payload)
