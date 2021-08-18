import os
import sys
import pandas as pd
from sqlalchemy import create_engine
import config

if __name__ == '__main__':

    """
        已有data.mapper.prepare_map_tables 替代，此脚本废弃
    """

    # pandas的一些设置，打印数据看起来舒服些
    # pandas.DataFrame显示所有列
    pd.set_option('display.max_columns', None)
    # pandas.DataFrame显示所有行
    pd.set_option('display.max_rows', None)
    # 设置pandas.DataFrame的value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    # 设置pandas.DataFrame 显示不换行
    pd.set_option('display.width', 10086)

    df = pd.read_excel('../data/hongfa_gw_map.xlsx.back')
    db_path = os.path.join(os.getcwd(), 'sqlite.db')

    conf = config.Config()
    engine = create_engine('sqlite:///%s' % conf.sqllite_file)
    df.to_sql('hongfa_gw_map', engine, index=True, if_exists='replace')
    # df.to_sql('hongfa_3p_map', engine, index=True)

    print(df.loc[0:])


    sys.exit()
    # 查看列的值分布
    print(df['key'].value_counts())
    print(df['address'].value_counts())