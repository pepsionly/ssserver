import json
import re
import sqlite3
from json import JSONDecodeError

from flask import Blueprint, request
from flask import current_app as app
from utils.hexconverter import HexConverter
from utils import CommonUtils
from config.const import Const
from utils.exceptions import *

consts = Const()
hongfa = Blueprint('/', __name__, template_folder='templates', static_folder='static')

"""
全局变量
hongfa_client = app.hongfa
mqtt_client = app.mqtt_client
conf = app.conf
mapper = app.mapper
redis_conn = app.redis_conn
"""


@hongfa.route('/gw/set', methods=['POST'])
def set_gw_params():
    """
    同时设定网关的多个参数
    @return:
    """

    device_brand = request.path.split('/')[1]
    post_data = json.loads(request.get_data())
    device_type = post_data.get('device_type')
    gid = post_data.get('gid')
    params = post_data.get('params')

    print(params)

    try:
        address_data_maps = app.mapper.map_data_address(params, device_brand, device_type)
    except sqlite3.OperationalError:
        raise DeviceTypeNotFound()


    for address_data_map in address_data_maps:
        topic, payload, request_hash = app.hongfa.gen_set_gw_data(gid, address_data_map['address'], address_data_map['data'])

        print(topic, payload, request_hash)
        app.mqtt_client.publish(topic, payload, qos=1)



    """
        sqlite3.OperationalError
    """
    return ''


@hongfa.route('/gw/set_one', methods=['POST'])
def gw_set_one_demo():
    data = request.get_data()
    data_obj = json.loads(data)

    brand = data_obj.get('brand')
    gid = data_obj.get('gid')
    param_id = data_obj.get('param_id')
    param_value = int(data_obj.get('interval') / 10)
    device_type = 'GW23'
    address_map = app.mapper.get_by_id(brand, device_type, param_id)

    address = address_map.get('address')[2:]  # 寄存器地址
    unit_address = 'FF'  # 网关通讯地址·
    function_code = '06'  # 功能码

    convert_fun = eval('HexConverter.%s_to_hex' % address_map.get('datatype'))
    data_hex = convert_fun(param_value).upper()

    # data_hex = HexConverter.ushort_to_hex(param_value).upper()

    len1 = int(4 + address_map['len'] * 2)  # 数据长度（加function_code和len2）
    hex_len = HexConverter.ushort_to_hex(len1)

    modbus_data_without_crc = address + hex_len + unit_address + function_code + address + data_hex
    print(modbus_data_without_crc)

    crc = CommonUtils.cal_modbus_crc16(modbus_data_without_crc)[2:].upper()
    modbus_data = crc + modbus_data_without_crc

    topic = 'hongfa/FFD1212006105728/download/'
    json_obj = {"GSN": gid, "GW_Request": modbus_data}
    json_str = json.dumps(json_obj).replace(' ', '')

    app.mqtt_client.publish(topic, json_str, 0)

    return 'set data %s to %s' % (param_id, str(int(param_value / 10)))
    return 'this is the gateway server!'


@hongfa.route('/<string:gid>/<string:pram_id>/<int:pram_value>')
def demo_set_report_interval(gid, pram_id, pram_value):
    """
    0XC2    0247    0006    ff              06              0247    0096
    CRC     address len1    unit_address    function_code   address data_hex
    @param gid:
    @param pram_name:
    @param pram_value:
    @return:
    """
    address = '0247'  # 寄存器地址
    len1 = '0006'  # 数据长度（加function_code和len2）
    unit_address = 'FF'  # 网关通讯地址
    function_code = '06'  # 功能码
    data_hex = HexConverter.ushort_to_hex(pram_value).upper()

    modbus_data_without_crc = address + len1 + unit_address + function_code + address + data_hex
    crc = CommonUtils.cal_modbus_crc16(modbus_data_without_crc)[2:].upper()
    modbus_data = crc + modbus_data_without_crc

    topic = 'hongfa/FFD1212006105728/download/'
    json_obj = {"GSN": gid, "GW_Request": modbus_data}
    json_str = json.dumps(json_obj).replace(' ', '')

    app.mqtt_client.publish(topic, json_str, 0)

    """
        {"GSN":"FFD1212006105728","GW_Request":"0DF202470006FF0602470096"}
    """
    return 'set data [report interval] to %s min' % str(int(pram_value / 10))


"""
以下注册统一的异常处理方法
"""


@hongfa.errorhandler(JSONDecodeError)
def handle_invalid_json(error):
    return json.dumps({"code": consts.INVALID_JSON_STRING, "msg": "invalid json string"})


@hongfa.errorhandler(BaseCustomException)
def handle_invalid_json(error):
    response = json.dumps(error.to_dict())
    return response
