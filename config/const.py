# -*- coding: utf-8 -*-
# python 3.x
# Filename:const.py
# 定义一个常量类实现常量的功能
#
# 该类定义了一个方法__setattr()__,和一个异常ConstError, ConstError类继承
# 自类TypeError. 通过调用类自带的字典__dict__, 判断定义的常量是否包含在字典
# 中。如果字典中包含此变量，将抛出异常，否则，给新创建的常量赋值。
# 最后两行代码的作用是把const类注册到sys.modules这个全局字典中。

class Const:

    class ConstError(TypeError):
        pass

    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise self.ConstError("Can't rebind const (%s)" % name)
        self.__dict__[name] = value

    def __init__(self):
        self.SUCCESS = 3203
        self.FAILED = 3204
        self.INVALID_PARAM_ID = 41  # ID未映射到参数或参数不可写
        self.INVALID_PARAM_VALUE = 42  # 参数值格式不正确
        self.INVALID_JSON_STRING = 43  # 非法的json字符串
        self.INVALID_DEVICE_TYPE = 44  # 未找到查询设备类型
        self.Request_Unhandled = 45  # 多次请求产生的key相同
        self.Response_Time_Out = 46  # 网关/断路器响应超时

