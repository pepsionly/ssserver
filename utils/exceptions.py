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

    def __init__(self, message='device type not found', status_code=consts.INVALID_DEVICE_TYPE, payload=None):
        BaseCustomException.__init__(self, message, status_code, payload)


class InvalidParamID(BaseCustomException):

    def __init__(self, message='invalid param id', status_code=consts.INVALID_PARAM_ID, payload=None):
        BaseCustomException.__init__(self, message, status_code, payload)


class InvalidParamValue(BaseCustomException):

    def __init__(self, message='invalid param value', status_code=consts.INVALID_PARAM_VALUE, payload=None):
        BaseCustomException.__init__(self, message, status_code, payload)
