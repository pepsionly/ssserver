import json
from json import JSONDecodeError

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
        """
        hongfa/FFD1212006105728/upload/订阅回调
        """
        # 主题
        topic = msg.topic

        # 数据
        msg_str = msg.payload.decode()
        try:
            msg_obj = json.loads(msg_str)
        except JSONDecodeError:
            logger.warning('unhandled msg: [%s] in topic：[%s]' % (msg_str, topic))
            return
        # 处理消息分类一：定时上报的数据
        if msg_obj.get('DataUp04'):
            self.handle_auto_report(topic, msg_obj)
        # 处理消息分类二：断路器上线
        elif msg_obj.get('SD_RAW_04'):
            self.handle_switch_online(topic, msg_obj)
        elif msg_obj.get('GW_Respond') or msg_obj.get('MRaw_Respond'):
            #计算哈希，回复数据，解锁
            self.handle_respond(topic, msg_obj)
        else:
            logger.warning('-=-=-=-=-=-=-=-=-=-=unhandled json_msg-=-=-=-=-=-=-=-=-=-=')
            logger.warning(msg_str)

    def handle_topic_will(self, client, userdata, msg):
        """
        hongfa/FFD1212006105728/will/订阅回调，处理网关下线，生成网关下线状态入库redis
        """
        # 主题
        topic = msg.topic
        # 更新网关在线状态
        self.redis_conn.hset(GatewayStatus({
            'brand': self.brand,
            'GID': topic.split('/')[1],
            'OL': '0'
        }))
        """
            mark 这里处理网关下线
        """

    def handle_respond(self, topic, msg_obj):
        """
        处理请求的响应，生成哈希，入库redis
        :param topic: hongfa/FFD1212006105728/upload/
        :param msg_obj: {'GSN': 'FFD1212006105728', 'GW_Respond': '4E3C02410006FF1002410007'}
                        {"SSN":"6B02210710091756","MRaw_Respond":"F31C0010000F02040C000000000000000000000000"}
        :return:
        """
        # 解锁网关请求
        self.unlock_gw(topic)

        # 网关ID
        gateway_id = topic.split('/')[1].strip()
        # 断路器ID
        switch_id = msg_obj.get('SSN')
        if switch_id:
            # msg_obj包含SSN就是断路器的响应
            modbus_data = msg_obj.get('MRaw_Respond')
            switch_status = self.redis_conn.hget(SwitchStatus({
                'brand': self.brand,
                'GID': gateway_id,
                'SID': switch_id
            }))
            if switch_status:
                device_code = switch_status.get('SDT')
                device_type, v2 = self.get_switch_device_type(device_code)
        elif msg_obj.get('GSN'):
            # msg_obj包含GSN就是网关的响应
            modbus_data = msg_obj.get('GW_Respond')
            gw_status = self.redis_conn.hget(GatewayStatus({
                'brand': self.brand,
                'GID': gateway_id,
            }))
            if gw_status:
                device_code = gw_status.get('GDT')
                device_type = self.get_gateway_device_type(device_code)

        key, fc = self.parse_respond_key(gateway_id, modbus_data)
        if fc == '03' or fc == '04':
            data = self.parse_tcp_data(modbus_data, topic=topic, msg_obj=msg_obj, device_type=device_type)
        else:
            data = {'result': 'success'}
        data_str = json.dumps(data)
        self.redis_conn.publish(key, data_str)
        # self.redis_conn.set(key, data_str, ex=60)

    def handle_switch_online(self, topic, msg_obj):
        """
        处理断路器上线，生成断路器和网关状态入库redis
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
        data_gw['OL'] = data_s['OL'] = 1
        data_s['brand'] = data_gw['brand'] = self.brand
        data_s['GID'] = data_gw['GID'] = gateway_id
        data_s = SwitchStatus(data_s).obj
        device_type, auto_data_count = self.get_switch_device_type(data_s['SDT'])
        data_s['ADC'] = auto_data_count
        self.redis_conn.hset(SwitchStatus(data_s))
        self.redis_conn.hset(GatewayStatus(data_gw))

    def handle_auto_report(self, topic, msg_obj):
        """
        处理断路器自动上传的数据，数据缓存到redis，同时维护断路器和网关的在线状态
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
        switch_status = self.redis_conn.hget(SwitchStatus({
            'brand': self.brand,
            'GID': gateway_id,
            'SID': switch_id
        }))
        if switch_status:
            device_type, auto_data_count = self.get_switch_device_type(int(switch_status['SDT']))
        if modbus_data[4:8] == '0000':
            device_ol = HexConverter.hex_to_bin(modbus_data[18:22])[0]
            new_switch_status = {'brand': self.brand,
                                 'SA': HexConverter.hex_to_utinyint(modbus_data[12:14]),
                                 'SID': switch_id,
                                 'GID': gateway_id,
                                 'OL': '0' if device_ol == '1' else 1,
                                 }
            if new_switch_status['OL'] == '0':
                """
                    mark 这里处理断路器下线
                """
            if not switch_status:
                # 若reporter中没有断路器类型代号，则尝试从开始地址为'0000'的数据中获取并保存到reporter中
                # 映射设备类型
                device_type_int = HexConverter.hex_to_ushort(modbus_data[34:38])
                device_type, auto_data_count = self.get_switch_device_type(device_type_int)
                new_switch_status.update({'SDT': device_type_int, 'ADC': auto_data_count})
            # 更新断路器状态
            self.redis_conn.hset(SwitchStatus(new_switch_status))


        # 01地址的0000条信息汇报同时更新网关在线状态（处理遗言常驻且内容不变）
        if modbus_data[12:14] == '01' and modbus_data[4:8] == '0000':
            self.redis_conn.hset(GatewayStatus({
                'brand': self.brand,
                'GID': topic.split('/')[1],
                'OL': '1'
            }))

        # 处理尚未获取到设备类型的情况
        if 'device_type' not in locals().keys():
            return
        # 解析 Modbus TCP 原始值
        result = self.parse_tcp_data(modbus_data, topic=topic, msg_obj=msg_obj, device_type=device_type)
        if not result:
            # 处理CRC验证出错
            return

        # 转化日期格式成'%Y-%m-%d %H:%M:%S'
        report_time = CommonUtils.standardize_datetime_210816144502(report_time)
        # 转化日期为时间戳
        time_stamp = CommonUtils.datetime_timestamp(report_time)
        """
            因为存在其他自动上传的数据与周期上传的数据相同
            所以这里还要判断是否到断路器数据的采集时间了
            不如建个mysql表存断路器和网关的对象吧
        """
        # 暂存断路器数据待处理：
        self.redis_conn.set_one(SwitchTempData({'brand': self.brand,
                                                'gateway_sn': gateway_id,
                                                'switch_sn': switch_id,
                                                'siblings': auto_data_count,
                                                'start_address': modbus_data[4:8],
                                                'date': time_stamp,
                                                'data': result,
                                                }))
        # 尝试合并、入库
        self.try_merge_data(self.brand, gateway_id, switch_id)

    def try_merge_data(self, brand, gateway_id, switch_id):
        # 取出暂存的值
        keys = 'temp:%s:%s:%s:*' % (brand, gateway_id, switch_id)
        temp_keys, temp_datas = self.redis_conn.get_all(keys, 3)
        temp_datas = [json.loads(item) for item in temp_datas]
        # 判断数据条数是否达到要求
        if not temp_datas or len(temp_datas) < temp_datas[0]['siblings']:
            return
        # 先排序
        # temp_datas = sorted(temp_datas, key=lambda x: x['start_address'])
        # 判断数据的时间戳是不是严格递增，先不用，会受其他自动上传的数据影响？：
        # if any(times[i + 1] <= times[i] for i in range(0, len(times) - 1)):
            # return
        # 判断数据的时间差
        times = [int(item['date']) for item in temp_datas]
        time_diff = max(times) - min(times)
        if time_diff >= 30:
            return

        # 入库合并的数据
        merged_data = {}
        for item in temp_datas:
            merged_data.update(dict((key, value) for key, value in item['data'].items() if 'id' not in key))
        # merged_data = dict((key, value) for key, value in merged_data.items() if 'id' not in key)

        data_report = SwitchData({
            'dtu_sn': temp_datas[0]['gateway_sn'],
            'date': temp_datas[0]['date'],
            'code': 1,
            'item': [{
                'cmp_sn1': temp_datas[0]['switch_sn'],
                'data': merged_data
            }],
        })
        self.redis_conn.left_push(data_report)
        # 还要清空temp下的key
        self.redis_conn.del_all(keys)

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

    def gen_switch_request(gen_func):
        """
        组装MRaw_Request json字符串的函数装饰器
        @param gen_func: 组装modbus原始值的函数
        @return:
            topic: 'hongfa/{gateway_id}/download/'
            payload: "{"GSN":"FFD1212006105728","GW_Request":"F1F302410015FF10024100070e000B000B000B000B03DE03E70064"}"
        """

        def gen_request(gid, sid, identifier, address, data):
            adu = gen_func(gid, sid, identifier, address, data)
            topic = 'hongfa/%s/download/' % str(gid)
            payload = {"SSN": sid, "MRaw_Request": adu}
            payload_str = json.dumps(payload).replace(' ', '')
            return topic, payload_str
        return gen_request

    @staticmethod
    @gen_switch_request
    def gen_switch_03h(gid, sid, identifier, address, register_count):
        return HongFa.gen_read(address, register_count, identifier=identifier, function_code='03')

    @staticmethod
    @gen_switch_request
    def gen_switch_04h(gid, sid, identifier, address, register_count):
        return HongFa.gen_read(address, register_count, identifier)

    @staticmethod
    @gen_switch_request
    def gen_switch_06h(gid, sid, identifier, address, hex_data):
        return HongFa.gen_06h(address, hex_data, identifier)

    @staticmethod
    @gen_switch_request
    def gen_switch_10h(gid, sid, identifier, address, hex_data):
        return HongFa.gen_10h(address, hex_data, identifier)

    def gen_gw_request(gen_func):
        """
        组装gw_request json字符串的函数装饰器
        @param gen_func: 组装modbus原始值的函数
        @return:
            topic: 'hongfa/{gateway_id}/download/'
            payload: "{"GSN":"FFD1212006105728","GW_Request":"F1F302410015FF10024100070e000B000B000B000B03DE03E70064"}"
        """

        def gen_request(gateway_id, address, hex_data):
            adu = gen_func(gateway_id, address, hex_data)
            topic = 'hongfa/%s/download/' % str(gateway_id)
            payload = {"GSN": gateway_id, "GW_Request": adu}
            payload_str = json.dumps(payload).replace(' ', '')
            return topic, payload_str

        return gen_request

    @staticmethod
    @gen_gw_request
    def gen_gw_03h(gateway_id, address, register_count):
        """
        生成读取宏发网关一个或多个输入寄存器的modbus原始值
        @param gateway_id: 网关GSN，供装饰器函数调用
        @param address: 起始寄存器地址
        @param register_count: 读取的寄存器数量
        @return: 见调用 gen_read(address, register_count)
        """
        return HongFa.gen_read(address, register_count, function_code='03')

    @staticmethod
    @gen_gw_request
    def gen_gw_04h(gateway_id, address, register_count):
        """
        生成读取宏发网关一个或多个输入寄存器的modbus原始值
        @param gateway_id: 网关GSN，供装饰器函数调用
        @param address: 起始寄存器地址
        @param register_count: 读取的寄存器数量
        @return: 见调用 gen_read(address, register_count)
        """
        return HongFa.gen_read(address, register_count)

    @staticmethod
    @gen_gw_request
    def gen_gw_06h(gateway_id, address, hex_data):
        """
        生成设定宏发网关一个参数的modbus原始值
        @param gateway_id: 网关id，供函数装饰器调用
        @param address: 起始寄存器地址
        @param hex_data: 数据
        @return: 见调用：gen_06h(address, hex_data)
        """
        return HongFa.gen_06h(address, hex_data)

    @staticmethod
    @gen_gw_request
    def gen_gw_10h(gateway_id, address, hex_data):
        """
        生成设定宏发网关多个参数的modbus原始值
        @param gateway_id: 网关GSN,
        @param address: 起始寄存器地址
        @param hex_data: 数据
        @return: 见调用：gen_10h(address, hex_data)
        """
        return HongFa.gen_10h(address, hex_data)

    @staticmethod
    def gen_read(address, register_count, identifier=255, function_code='04'):
        """
        生成读取宏发网关/断路器一个或多个寄存器的modbus原始值
        @param address: 起始寄存器地址
        @param register_count: 读取的寄存器数量
        @param identifier: 设备的单位标识符
        @param function_code: 功能码默认'04'读输入寄存器, 传入03可读保持寄存器
        @return: adu 完整的 modbus 原始值
            MBAP = CRC检验码 + 起始寄存器地址 + 以下数据的字节数 + 单元标识符
            MBAP = F1F3     + 0241        + 0015          + FF
            adu = MBAP + 功能码 + 起始寄存器地址 + 寄存器数量
            adu = MBAP + 04    + 0241        + 0007
        """
        identifier_hex = HexConverter.ushort_to_hex(identifier)[2:].upper()
        register_count_hex = HexConverter.ushort_to_hex(register_count).upper()
        pdu = function_code + address + register_count_hex
        adu_without_crc = address + '0006' + identifier_hex + pdu
        crc = CommonUtils.cal_modbus_crc16(adu_without_crc)[2:].upper()
        return crc + adu_without_crc

    @staticmethod
    def gen_06h(address, hex_data, identifier=255):
        """
        生成设定宏发网关/断路器一个参数的modbus原始值
        @param address: 起始寄存器地址
        @param hex_data: 数据
        @param identifier: 设备的单位标识符
        @return: adu 完整的 modbus 原始值
            MBAP = CRC检验码 + 起始寄存器地址 + 以下数据的字节数 + 单元标识符
            MBAP = F1F3     + 0241        + 0006          + FF
            adu = MBAP + 功能码 + 起始寄存器地址 + 数据
            adu = MBAP + 06    + 0241        + 000B
        """

        identifier_hex = HexConverter.ushort_to_hex(identifier)[2:].upper()
        pdu = '06' + address + hex_data
        adu_without_crc = address + '0006' + identifier_hex + pdu
        crc = CommonUtils.cal_modbus_crc16(adu_without_crc)[2:].upper()
        return crc + adu_without_crc

    @staticmethod
    def gen_10h(address, hex_data, identifier=255):
        """
        生成设定宏发网关/断路器多个参数的modbus原始值
        @param address: 起始寄存器地址
        @param hex_data: 数据
        @param identifier: 设备的单元标识符
        @return: adu 完整的 modbus 原始值
            MBAP = CRC检验码 + 起始寄存器地址 + 以下数据的字节数 + 单元标识符
            MBAP = F1F3     + 0241        + 0015          + FF
            adu = MBAP + 功能码 + 起始寄存器地址 + 寄存器数量 + 数据字节数 + 数据
            adu = MBAP + 10    + 0241        + 0007     + 0E       + 000B000B000B000B03DE03E70064
        """
        identifier_hex = HexConverter.ushort_to_hex(identifier)[2:].upper()
        bytes_count_data = HexConverter.ushort_to_hex(int(len(hex_data) / 2))[2:].upper()
        register_count = HexConverter.ushort_to_hex(int(len(hex_data) / 4)).upper()
        function_code = '10'
        hex_data_with_gateway_address = identifier_hex + function_code + address + register_count \
                                        + bytes_count_data + hex_data

        bytes_count_with_gateway_address = HexConverter.ushort_to_hex(
            int(len(hex_data_with_gateway_address) / 2)).upper()

        adu_without_crc = address + bytes_count_with_gateway_address + hex_data_with_gateway_address

        crc = CommonUtils.cal_modbus_crc16(adu_without_crc)[2:].upper()

        #  返回完整的modbus原始值
        return crc + adu_without_crc

    @staticmethod
    def parse_respond_key(gateway_id, modbus_data):
        """
        @param gateway_id:
        @param modbus_data:
        10H : {'GSN': 'FFD1212006105728', 'GW_Respond': '4E3C 0241 0006 FF 10 0241 0007'} 写成功0007个寄存器
        06H : {'GSN': 'FFD1212006105728', 'GW_Respond': '195A01520006FF0601520822'} 写成功0001个寄存器
        03/04H: {"SSN":"6B02210710091756","MRaw_Respond":"F31C 0010 000F 02 04 0C 000000000000000000000000"} 读成功000C个寄存器
        @return: 暂存结果的 redis key和功能码
        """
        unit_identifier = modbus_data[12:14]
        fc = modbus_data[14:16]
        address = modbus_data[4:8]
        # 数据长度默认
        register_count_hex = ''
        if fc == '10':
            register_count_hex = modbus_data[-4:]
        elif fc == '06':
            register_count_hex = '0001'
        elif fc == '03' or fc == '04':
            register_count_int = HexConverter.hex_to_ushort('00' + modbus_data[16:18]) / 2
            register_count_hex = HexConverter.ushort_to_hex(register_count_int)

        key = ':'.join(['responds', gateway_id, unit_identifier, fc, address, register_count_hex])
        return key, fc

    @staticmethod
    def parse_request_key(topic, payload):
        """
        @param topic:
        @param payload:
        @return:
        """
        """
            MBAP = CRC检验码 + 起始寄存器地址 + 以下数据的字节数 + 单元标识符
            MBAP = F1F3     + 0241        + 0015          + FF
            adu = MBAP + 功能码 + 起始寄存器地址 + 寄存器数量
            adu = MBAP + 04    + 0241        + 0007
        """
        # 网关ID
        gateway_id = topic.split('/')[1].strip()
        payload_obj = json.loads(payload)
        modbus_data = payload_obj.get('MRaw_Request')
        if not modbus_data:
            modbus_data = payload_obj.get('GW_Request')
        if not modbus_data:
            # 处理非 MRaw_Request 和 Gw_Request
            return None, None
        unit_identifier = modbus_data[12:14]
        fc = modbus_data[14:16]
        address = modbus_data[4:8]
        # 数据长度默认
        register_count_hex = ''
        if fc == '10':
            register_count_hex = modbus_data[20:24]
        elif fc == '06':
            register_count_hex = '0001'
        elif fc == '03' or fc == '04':
            """
                adu = MBAP + 功能码 + 起始寄存器地址 + 寄存器数量
                adu = MBAP + 04    + 0241        + 0007
            """
            register_count_int = HexConverter.hex_to_ushort(modbus_data[-4:])
            register_count_hex = HexConverter.ushort_to_hex(register_count_int)
        key = ':'.join(['responds', gateway_id, unit_identifier, fc, address, register_count_hex])
        return key, fc

    def unlock_gw(self, topic):
        topic = topic.replace('upload', 'download')
        key = 'locks:' + topic
        self.redis_conn.delete(key)

    @staticmethod
    def get_switch_device_type(code):
        """
        :param code: 5
        :return: (断路器的设备类型, 断路器自动上传的数据条数)
        """
        mapper = {1: ('1P', 0), 2: ('1PN', 0), 3: ('1PNL', 3),
                  4: ('3P', 0), 5: ('3PN', 4), 6: ('3PNL', 0),
                  12: ('2P', 0), 16: ('4P', 0)}
        return mapper.get(int(code))



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
