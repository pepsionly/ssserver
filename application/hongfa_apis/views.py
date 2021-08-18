import json

from flask import Blueprint
from flask import current_app as app
from utils.hexconverter import HexConverter
from utils import CommonUtils

hongfa = Blueprint('/', __name__, template_folder='templates', static_folder='static')

"""全局变量
mqtt_client = app.mqtt_client
conf = app.conf
mapper = app.mapper
redis_conn = app.redis_conn
"""


@hongfa.route('/')
def index():
    brand = 'hongfa'
    device_type = 'GW23'
    id = 'id159'

    address_map = app.mapper.get_by_id(brand, device_type, id)

    return json.dumps(address_map)
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
    return 'set data [report interval] to %s min' % str(int(pram_value/10))
