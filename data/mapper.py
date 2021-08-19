import os
import re
import sqlite3
import struct
import pandas as pd
from config.dev import DevelopmentConfig
from utils import CommonUtils
from utils.hexconverter import HexConverter
from config.const import Const
from utils.exceptions import *
const = Const()


class Mapper(object):

    def __init__(self):
        # 加载配置文件
        self.conf = DevelopmentConfig()
        # 初始化sqlite数据库连接
        self.sqlite_conn = sqlite3.connect(os.path.abspath(self.conf.sqllite_file), check_same_thread=False)
        # 开发配置下从新准备映射数据到sqlite（覆盖）
        if isinstance(self.conf, DevelopmentConfig):
            self.prepare_map_tables('../data/')

    def __del__(self):
        # 清理数据库连接
        self.sqlite_conn.close()

    def zip_col_name_and_row(query_func):
        """
        装饰器函数，负责从指针中提取表头，并把之与一条记录打包成字典
        :param query_func: get_by_address(self, brand, device_type, address)
               query_func返回一条记录(row)和指针(cursor)
        :return: 由数据库表头和记录打包成的字典
        """

        def zip_up(self, brand, device_type, address, write):
            row, cursor = query_func(self, brand, device_type, address, write)
            if row:
                # 获取表头
                col_name_list = [tuple[0] for tuple in cursor.description]
                # 打包成字典
                return dict(zip(col_name_list, row))
            else:
                return None

        return zip_up

    @zip_col_name_and_row
    def get_by_id(self, brand, device_type, param_id, write=False):
        """
        表名同config目录下的xlsx文件
        :param param_id: id00
        :param brand: hongfa/timu
        :param device_type: GW/1PNL/3PN
        @param write: 是否要求参数可写
        :return: row, cursor交由装饰器函数提取表头并打包成字典
        """
        if not write:
            # 按地址查询,device_type有些事数字开头不能做表头，所以统一在前面在'A', 比如'A3PN=1'表示3PN设备支持对应行的变量
            sql = "SELECT * FROM %s_%s_map WHERE id = '%s' AND A%s=1"
            cursor = self.sqlite_conn.execute(sql % (brand, device_type[0:2], param_id, device_type))
        else:
            # 要求参数可写的查询
            sql = "SELECT * FROM %s_%s_map WHERE id = '%s' AND A%s=1 and write=1"
            cursor = self.sqlite_conn.execute(sql % (brand, device_type[0:2], param_id, device_type))

        for row in cursor:
            return row, cursor
        # 空查询返回None
        return None, None

    @zip_col_name_and_row
    def get_by_address(self, brand, device_type, address, write=False):
        """
        表名同config目录下的xlsx文件
        :param address: 0x0000
        :param brand: hongfa/timu
        :param device_type: GW/1PNL/3PN
        :param write: 给装饰器函数的占位参数
        :return: row, cursor交由装饰器函数提取表头并打包成字典
        """
        # 按地址查询,device_type有些事数字开头不能做表头，所以统一在前面在'A', 比如'A3PN=1'表示3PN设备支持对应行的变量
        sql = "SELECT * FROM %s_%s_map WHERE address = '%s' AND A%s=1"
        cursor = self.sqlite_conn.execute(sql % (brand, device_type[0:2], address, device_type))
        for row in cursor:
            return row, cursor
        # 空查询返回None
        return None, None

    def map_address_data(self, first_address, data_hex, brand, device_type):
        """
        @param data_hex: 需映射的16进制数据字符串
                        例：'000500026B02210710091756'
        @param first_address: data_hex首对字节在设备上的起始地址
                            例：'000D'
        @param brand: 设备品牌'hongfa'/'timu'
        @param device_type: GW23/1PNL/3PN等，详见data目录下的xxxxxx_xx_map.xlsx映射文件
        @return: 变量id-value字典
        """
        data_count = int(len(data_hex) / 4)
        address_list = CommonUtils.get_address_list(first_address, data_count)
        data_list = CommonUtils.cut_text(data_hex, 4)
        result = {}
        # 开始查库映射
        for address in address_list:
            address_map = self.get_by_address(brand, device_type, address, False)
            if address_map:
                data_index = address_list.index(address)
                data_hex = ''.join(data_list[data_index: data_index + int(address_map['len'])])
                # 进制转换
                data = eval('HexConverter.hex_to_%s(data_hex)' % address_map['datatype'].strip())
                # 处理小数点: Y/100
                if address_map.get('format'):
                    times = re.search('Y/([\d]+)', address_map['format'])
                    if times:
                        times = float(times[1])
                        data = data / times
                result[address_map['id']] = data
        return result

    def map_data_address(self, param_dict, brand, device_type, write=False):
        """
        @param param_dict: 需解析成 寄存器地址-16进制数据字典
        例：param_dict = {
                        'id113': '1730',  # 网关时间：分秒
                        'id114': '1231',  # 网关时间：日时
                        'id115': '2018',  # 网关时间：年月
                        }
        @param brand: 设备品牌'hongfa'/'timu'
        @param device_type: GW23/1PNL/3PN等，详见data目录下的xxxxxx_xx_map.xlsx映射文件
        @param write: 映射的参数时用于写还是读
        @return:
        """
        result_data = []
        for param_id, param_value in param_dict.items():
            address_map = self.get_by_id(brand, device_type, param_id, write)
            if not address_map:
                # 抛出未找到映射记录的param_id的异常,待处理
                raise InvalidParamID
            else:
                convert_fun = eval('HexConverter.%s_to_hex' % address_map.get('datatype'))
                # 处理小数点: Y/100
                if address_map.get('format'):
                    times = re.search('Y/([\d]+)', address_map['format'])
                    if times:
                        times = int(times[1])
                        param_value = param_value * times
                try:
                    param_hex = convert_fun(param_value).upper()
                except struct.error:
                    #  抛出参数格式不正确的异常,待处理
                    raise InvalidParamValue

                result_data.append({'address': address_map['address'],
                                    'param_hex': param_hex})
        return self.zip_address_hex_data(result_data)

    def zip_address_hex_data(self, data):
        """
        找出所有连续地址的开头，和对应的十六进制数据
        @param data: ：[{'address': '0x0150', 'param_hex': '06C2'}, {'address': '0x0151', 'param_hex': '04CF'}, ...]
        @return:
        """
        # 排序数据
        data_sorted = sorted(data, key=lambda x: x['address'])
        # 取出所有地址并转化成整数/和16进制数据
        address_int_sorted = [HexConverter.hex_to_ushort(i['address'][2:]) for i in data_sorted]
        data_hex_sorted = [i['param_hex'] for i in data_sorted]
        # 初始化结果数组
        result_data = []
        # 找出所有连续地址的开头
        last_index = len(data_hex_sorted)
        while address_int_sorted:
            current_address_int = address_int_sorted.pop()
            is_start_address = True if not address_int_sorted else current_address_int != address_int_sorted[-1] + 1

            if is_start_address:
                current_index = len(address_int_sorted)
                result_data.append({
                    'address': HexConverter.ushort_to_hex(current_address_int),
                    'data': ''.join(data_hex_sorted[current_index: last_index])
                })
                last_index = current_index
        print(result_data)
        return result_data

    @staticmethod
    def get_file_list(directory, filetype):  # 输入路径、文件类型例如'.csv'
        """
        获取directory目录下的所有filetype后缀的文件名列表
        :param directory: 文件目录
        :param filetype:  文件后缀如.xlsx
        :return:
        """
        file_list = []
        for root, dirs, files in os.walk(directory):
            for i in files:
                if os.path.splitext(i)[1] == filetype and not i.startswith('~$'):
                    file_list.append(i)
        return file_list  # 输出由有后缀的文件名组成的列表

    def prepare_map_tables(self, directory='../data/'):
        """
        把directory目录下的所有点位变量映射表格添加到sqlite数据库
        :return:None
        """
        table_files = self.get_file_list(directory, '.xlsx')
        for table_file in table_files:
            # 这里文件未关闭，生产环境不读xlsx问题不大
            df = pd.read_excel(directory + table_file)
            table_name = table_file.split('.')[0]
            df.to_sql(table_name, self.sqlite_conn, index=True, if_exists='replace')
