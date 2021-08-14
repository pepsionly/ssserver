import os
import re
import sqlite3
import pandas as pd
from config.dev import DevelopmentConfig
from utils import CommonUtils
from utils.hexconverter import HexConverter

class Mapper(object):

    def __init__(self):
        self.conf = DevelopmentConfig()
        self.sqlite_conn = sqlite3.connect(os.path.abspath(self.conf.sqllite_file), check_same_thread=False)
        if isinstance(self.conf, DevelopmentConfig):
            pass
            # self.prepare_map_tables('../data/')

    def __del__(self):
        self.sqlite_conn.close()

    def zip_col_name_and_row(query_func):
        """
        装饰器函数，负责从指针中提取表头，并把之与一条记录打包成字典
        :param query_func: get_by_address(self, brand, device_type, address)
               query_func返回一条记录(row)和指针(cursor)
        :return: 由数据库表头和记录打包成的字典
        """
        def zip_up(self, brand, device_type, address):
            row, cursor = query_func(self, brand, device_type, address)
            if row:
                # 获取表头
                col_name_list = [tuple[0] for tuple in cursor.description]
                # 打包成字典
                return dict(zip(col_name_list, row))
            else:
                return None
        return zip_up

    @zip_col_name_and_row
    def get_by_address(self, brand, device_type, address):
        """
        表名同config目录下的xlsx文件
        :param address: 0x0000
        :param brand: hongfa/timu
        :param device_type: gateway/1p/3p
        :return: row, cursor交由装饰器函数提取表头并打包成字典
        """
        # 按地址查询
        sql = "SELECT * FROM %s_%s_map WHERE address = '%s'"
        cursor = self.sqlite_conn.execute(sql % (brand, device_type, address))
        for row in cursor:
            return row, cursor
        # 空查询返回None
        return None, None

    def map_address_data(self, first_address, data_hex, brand, device_type):
        data_count = int(len(data_hex) / 4)
        address_list = CommonUtils.get_address_list(first_address, data_count)
        data_list = CommonUtils.cut_text(data_hex, 4)

        result = {}
        # 开始查库映射
        for address in address_list:
            address_map = self.get_by_address(brand, device_type, address)
            if address_map:
                data_index = address_list.index(address)
                data_hex = ''.join(data_list[data_index: data_index + int(address_map['len'])])
                # 进制转换
                data = eval('HexConverter.hex_to_%s(data_hex)' % address_map['datatype'].strip())
                # 处理小数点: Y/100
                if address_map.get('format'):
                    multi_times = re.match('Y/([\d]+)', address_map['format'])
                    if multi_times:
                        multi_times = float(multi_times[1])
                        data = data / multi_times
                result[address_map['key']] = data

        return result


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
            df = pd.read_excel(directory + table_file)
            table_name = table_file.split('.')[0]
            df.to_sql(table_name, self.sqlite_conn, index=True, if_exists='replace')


