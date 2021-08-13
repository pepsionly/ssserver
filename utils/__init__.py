# -*- coding: utf_8 -*-
import re


import logging
from collections import Iterable


def error_management(func):
    def func_wrapper(*args, **kwargs):
        try:
            logging.info("Call %r", func.__name__)
            return func(*args, **kwargs)
        except:
            logging.error("Invalid value received")

    return func_wrapper


class CommonUtils(object):

    @staticmethod
    def hex_to_int(s):
        if not s.startswith('0x'):
            s = '0x' + s
        i = int(s, 16)  # convert from hex to a Python int
        return i

        """cp = pointer(c_int(i))  # make this into a c integer
        fp = cast(cp, POINTER(c_float))  # cast the int pointer to a float pointer
        return fp.contents.value  # dereference the pointer, get the float"""

    @staticmethod
    def array_reverse_iterator(items):
        """
        翻转数组，返回生成器
        :param items: 可迭代对象
        :return: 翻转数组的生成器
        """
        assert isinstance(items, Iterable)
        items.reverse()
        for text in items:
            yield text

    @staticmethod
    def cut_text(text, lenth):
        """
        :param text: 任意字符串
        :param lenth: 任意整形
        :return: 把text按length切分的字符串数组，顺序倒置，按生成器返回，
        """
        text_arr = re.findall('.{' + str(lenth) + '}', text)
        if len(''.join(text_arr)) < len(text):
            text_arr.append(text[(len(text_arr) * lenth):])
        return text_arr



    @staticmethod
    def get_address_list(first_address, count):
        """
        :param first_address:第一个16进制字符(2C,or 0x2C)
        :param count: 总共要几个16进制字符
        :return:
        """
        if not first_address.startswith('0x'):
            first_address = '0x' + first_address

        address_list = [first_address]

        for i in range(1, count):
            first_address_int = int(first_address, 16)
            new_address_int = first_address_int + i
            # 补位
            new_address_hex = "0x%04x" % new_address_int
            address_list.append(new_address_hex)
        return address_list
    
    @staticmethod
    def cal_modbus_crc16(string):
        data = bytearray.fromhex(string)
        crc = 0xFFFF
        for pos in data:
            crc ^= pos
            for i in range(8):
                if ((crc & 1) != 0):
                    crc >>= 1
                    crc ^= 0xA001
                else:
                    crc >>= 1

        return hex(((crc & 0xff) << 8) + (crc >> 8))


