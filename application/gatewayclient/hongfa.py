import json

from application.gatewayclient import GatewayClient
from utils.__init__ import CommonUtils
from utils.log import Log
from application.redisclient.model.models import *
from application.redisclient import RedisConn

logger = Log(__name__).getlog()
from utils.hexconverter import HexConverter


class HongFa(GatewayClient):

    def __init__(self, conf, redis_conn, mapper):
        super().__init__(conf)
        self.set_subscribe_topics()
        self.set_topic_handlers()
        self.mapper = mapper
        self.brand = 'hongfa'
        assert isinstance(redis_conn, RedisConn)
        self.redis_conn = redis_conn

    def __del__(self):
        self.redis_conn.close()

    def set_subscribe_topics(self):
        self.subscribe_topics = self.conf.hongfa_subscribe

    """def set_topic_handlers(self):
        self.topic_handlers = [self.handle_topic_uploads]"""

    def handle_topic_uploads(self, client, userdata, msg):
        # 数据
        msg_str = msg.payload.decode()
        msg_obj = json.loads(msg_str)
        # 主题
        topic = msg.topic
        # 处理消息分类一：定时上报的数据
        if msg_obj.get('DataUp04'):
            self.handle_auto_report(topic, msg_obj)
        # 处理消息分类二：断路器上线
        elif msg_obj.get('SD_RAW_04'):
            self.handle_switch_online(topic, msg_obj)

        else:
            print(msg_obj)

    def handle_topic_will(self, client, userdata, msg):
        # 数据
        msg_str = msg.payload.decode()
        # 主题
        topic = msg.topic
        print(topic)
        print(msg_str)

    def handle_switch_online(self, topic, msg_obj):
        """
        :param topic: hongfa/FFD1212006105728/upload/
        :param msg_obj: {"GWD_RAW_04":"D3F60000001100040E001700020002FFD1212006105728","SD_RAW_04":"63FD000D000F00040C000500026B02210710091756"}
        :return:
        """
        # 网关ID
        gateway_id = topic.split('/')[1].strip()
        # 网关 Modbus TCP 原始值
        modbus_data_gw = msg_obj.get('GWD_RAW_04')
        # 断路器 Modbus TCP 原始值
        modbus_data_s = msg_obj.get('SD_RAW_04')

        # 汇报断路器上线/网关状态/网关和断路器设备类型
        # 截取网关设备类型代号
        gw_device_code = HexConverter.hex_to_ushort(modbus_data_gw[18:22])
        # 映射网关设备类型
        gw_device_type = self.get_gateway_device_type(gw_device_code)
        # 解析数据
        data_gw = self.parse_tcp_data(modbus_data_gw, topic=topic, msg_obj=msg_obj, device_type=gw_device_type)
        data_s = self.parse_tcp_data(modbus_data_s, topic=topic, msg_obj=msg_obj, device_type=gw_device_type)

        # 完善并上报
        data_s['OL'] = data_gw['OL'] = True
        data_s['brand'] = data_gw['brand'] = self.brand
        data_s['GID'] = gateway_id
        self.redis_conn.set_one(SwitchStatus(data_s))
        self.redis_conn.set_one(GatewayStatus(data_gw))

    def handle_auto_report(self, topic, msg_obj):
        """
        :param topic: hongfa/FF3D210725113600/upload/
        :param msg_obj: {"SSN":"4C36210605133128","DataUp04":"76B80000002F01042C00000000000000000003000100010001003F000000000000003C3797FFDB9045000000000000435DC9280025","TM":"220105165657"}
        :return:
        """
        # 上报时间
        report_time = msg_obj.get('TM')
        # 网关ID
        gateway_id = topic.split('/')[1].strip()
        # 断路器ID
        switch_id = msg_obj.get('SSN')
        # Modbus TCP 原始值
        modbus_data = msg_obj.get('DataUp04')

        # 先从reporter(redis等)获取断路器类型代号
        switch_status = self.redis_conn.get_one(SwitchStatus({
            'brand': self.brand,
            'GID': gateway_id,
            'SID': switch_id
        }))
        if switch_status:
            device_type = self.get_switch_device_type(switch_status['SDT'])
        elif modbus_data[4:8] == '0000':
            # 若reporter中没有断路器类型代号，则尝试从开始地址为'0000'的数据中获取并保存到reporter中
            device_type = self.get_switch_device_type(HexConverter.hex_to_ushort(modbus_data[34:38]))
            self.redis_conn.set_one(SwitchStatus({'brand': self.brand,
                                                  'SDT': HexConverter.hex_to_ushort(modbus_data[34:38]),
                                                  'SA': HexConverter.hex_to_utinyint(modbus_data[12:14]),
                                                  'SID': switch_id,
                                                  'GID': gateway_id,
                                                  'OL': True,
                                                  }))

        # 解析 Modbus TCP 原始值
        result = self.parse_tcp_data(modbus_data, topic=topic, msg_obj=msg_obj, device_type=device_type)

        # 转化日期格式成'%Y-%m-%d %H:%M:%S'
        report_time = CommonUtils.standardize_datetime_210816144502(report_time)
        # 转化日期为时间戳
        time_stamp = CommonUtils.datetime_timestamp(report_time)
        if not result:
            # 处理CRC验证出错
            # return None
            pass
        return self.redis_conn.left_push(SwitchData({'brand': self.brand,
                                                     'gateway_id': gateway_id,
                                                     'switch_id': switch_id,
                                                     'date': time_stamp,
                                                     'code': 1,
                                                     'data': result,
                                                     }))

    def validate_crc_code(parse_func):
        """
        crc校验的函数装饰器
        :return:
        """

        def validate(self, data, **kwargs):
            validate_result = HongFa._validate_crc_code(data)
            if validate_result:
                return parse_func(self, data, kwargs=kwargs)
            else:
                # 在这里处理crc校验失败的业务, 通过kwargs在'parse_tcp_data'等函数传入传入相关参数
                logger.warning('CRC Validation Failed:' + json.dumps(kwargs))

        return validate

    @staticmethod
    def _validate_crc_code(data):
        """
        CRC校验Modbus TCP原始值
        :param data:
        :return: 0 if vilidated
        """
        # 宏发的数据CRC在开头，需要换到结尾
        crc_code = data[0:4]
        standard_data = data[4:] + crc_code
        result = CommonUtils.cal_modbus_crc16(standard_data)
        result = True if int(result[2:], 16) == 0 else False
        return result

    @validate_crc_code
    def parse_tcp_data(self, data, **kwargs):
        """
        :param data: "DataUp04":"76B80000002F01042C00000000000000000003000100010001003F000000000000003C3797FFDB9045000000000000435DC9280025"
        展开
        9DA50016002F01  04      2C          402C0EA43B6147AE13880000013B00014C36210605133128186BDD5E000000000000000000000000845200C1 1P
        9CA0002C002B01  04      28          542CEE596AD453D80001000100000000000000000000000000000000000100010000FFFF3B6147AE         1P
        3A950000002F02  04      2C          000000000000000000050001003A0001000000000000000000000000003F0000000000000000000000000000 3P
        MBAP报文头       功能码   数据长度      数据
        MBAP报文头:
        9DA5     0016    002F    01
        9CA0     002C    002B    01
        3A95     0000    002F    02
        CRC检验码 起始寄存器地址 长度 单元标识符
        :param kwargs: 预留参数，后续在crc校验的函数装饰器内做crc校验出错处理可能需要
        :return:
        """
        # 第一个数据的寄存器地址
        first_address = data[4:8]
        # 读到的寄存器数量
        data_len = data[16:18]
        data_len = CommonUtils.hex_to_int(data_len)
        # 十六数据
        data_hex = data[18:]
        if kwargs['kwargs'].get('device_type'):
            result = self.mapper.map_address_data(first_address, data_hex, self.brand,
                                                  kwargs['kwargs'].get('device_type'))
        else:
            result = None
        return result

    def gen_set_gw_data(self, gateway_id, address, hex_data):
        """
        @param gateway_id:
        @param address:
        @param hex_data:
        @return:
        """

        """
         register_count=len(hex_data)/4     byte_count=len(hex_data)/2  hex_data
        
        """
        # hex_data
        bytes_count_data = HexConverter.ushort_to_hex(int(len(hex_data)/2))[2:]
        register_count = HexConverter.ushort_to_hex(int(len(hex_data)/4))
        # address
        function_code = '10'
        gateway_address = 'FF'
        hex_data_with_gateway_address = gateway_address + function_code + address + register_count \
                                         + bytes_count_data + hex_data

        bytes_count_with_gateway_address = HexConverter.ushort_to_hex(int(len(hex_data_with_gateway_address)/2)).upper()

        hex_data_without_crc = address + bytes_count_with_gateway_address + hex_data_with_gateway_address

        crc = CommonUtils.cal_modbus_crc16(hex_data_without_crc)[2:].upper()

        modbus_data = crc + hex_data_without_crc

        topic = 'hongfa/%s/download/' % str(gateway_id)

        payload = {"GSN": gateway_id, "GW_Request": modbus_data}
        payload_str = json.dumps(payload).replace(' ', '')

        return topic, payload_str, ''

    @staticmethod
    def get_switch_device_type(code):
        """
        :param code: 5
        :return: 断路器的设备类型
        """
        mapper = {1: '1P', 2: '1PN', 3: '1PNL',
                  4: '3P', 5: '3PN', 6: '3PNL',
                  12: '2P', 16: '4P'}
        return mapper.get(code)

    @staticmethod
    def get_gateway_device_type(device_code):
        """
        :param device_code:
            21：WiFi版本网关
            22：以太网RJ45网关
            23: 4G网关
            24: 4G网关路由
        :return: 'GW' + 'device_code'
        """
        return 'GW' + str(device_code)
