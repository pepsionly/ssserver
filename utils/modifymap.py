# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import pandas as pd
from hexconverter import HexConverter
# pandas的一些设置，打印数据看起来舒服些
# pandas.DataFrame显示所有列
pd.set_option('display.max_columns', None)
# pandas.DataFrame显示所有行
pd.set_option('display.max_rows', None)
# 设置pandas.DataFrame的value的显示长度为100，默认为50
pd.set_option('max_colwidth', 100)
# 设置pandas.DataFrame 显示不换行
pd.set_option('display.width', 10086)


def add_32_device():
    df = pd.read_excel('hongfa_gw_map.xlsx')
    for i in range(2, 33):
        i1 = i * 6 + 1
        i2 = i1 + 1
        i3 = i1 + 2
        addres1 = '0x' + HexConverter.ushort_to_hex(i1).upper()
        addres2 = '0x' + HexConverter.ushort_to_hex(i2).upper()
        addres3 = '0x' + HexConverter.ushort_to_hex(i3).upper()
        row1 = [addres1, '第[%s]个从机设备类型' % str(i), 'ushort', '0x04', '', 1, 'SDT%s' % str(i), 'SDT', 0, 1]
        row2 = [addres2, '第[%s]个从机设备地址' % str(i), 'ushort', '0x04', '', 1, 'SA%s' % str(i), 'SA', 0, 1]
        row3 = [addres3, '第[%s]个从机设备设备ID号' % str(i), 'hex', '0x04', '', 4, 'SID%s' % str(i), 'SID', 0, 1]
        df.loc[len(df)] = row1
        df.loc[len(df)] = row2
        df.loc[len(df)] = row3
    df.to_excel('test.xlsx')

def add_function_code():
    df = pd.read_excel('../data/hongfa_3p_map.xlsx.back')

    for x in df.index:
        df.loc[x, "rir"] = 1 if '0x03' in str(df.loc[x, "functioncode"]) else 0
        df.loc[x, "rhr"] = 1 if '0x04' in str(df.loc[x, "functioncode"]) else 0
        df.loc[x, "psr"] = 1 if '0x06' in str(df.loc[x, "functioncode"]) else 0
        df.loc[x, "pmr"] = 1 if '0x10' in str(df.loc[x, "functioncode"]) else 0

    print('写了吗啊')
    df.to_excel('../data//hongfa_3p_map.xlsx')



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    add_function_code()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/