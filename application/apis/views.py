import json

from flask import Blueprint
from flask import current_app as app
from utils.hexconverter import HexConverter
from utils import CommonUtils

index_blu = Blueprint('/', __name__, template_folder='templates', static_folder='static')

"""全局变量
mqtt_client = app.mqtt_client
conf = app.conf
mapper = app.mapper
redis_conn = app.redis_conn
"""

@index_blu.route('/')
def index():
    topic = 'hongfa/FFD1212006105728/upload/'
    payload = '{"GWD_RAW_04":"D3F60000001100040E001700020002FFD1212006105728","SD_RAW_04":"63FD000D000F00040C000500026B02210710091756"}'
    # payload = '{"GWD_RAW_04":"D3F60000001100040E001700020002FFD1212006105728","SD_RAW_04":"FE290007000F00040C000300014C36210605133128"}'
    qos = 0
    app.mqtt_client.publish(topic, payload, qos)

    return 'this is the gate way server!'


@index_blu.route('/hongfa/<string:gid>/<string:pram_id>/<int:pram_value>')
def write_gw_single_pram(gid, pram_id, pram_value):
    """
    3A95    0000    002F    01              06              02   0000
    CRC     address len1    unit_address    function_code   len2
    @param gid:
    @param pram_name:
    @param pram_value:
    @return:
    """

    address = '0247'
    len1 = '04'
    unit_address = '01'
    function_code = '06'
    len2 = '02'
    data_hex = HexConverter.ushort_to_hex(pram_value)

    modbus_data_without_crc = address + len1 + unit_address + function_code + len2 + data_hex

    crc = CommonUtils.cal_modbus_crc16(modbus_data_without_crc)

    modbus_data = crc + modbus_data_without_crc

    topic = 'hongfa/FFD1212006105728/download/'
    json_obj = {"GSN": gid, "GW_Request": modbus_data}
    json_str = json.dumps(json_obj)

    print(json_str)
    app.mqtt_client.publish(topic, json_str, 0)

    return 'set data [report interval] to %s min' % str(pram_value)
