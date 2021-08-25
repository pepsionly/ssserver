import copy
import re

from application.redisclient.model import RedisColumn, RedisModel


class SwitchStatus(RedisModel):
    """
        {
            brand： hongfa/timu
            SDT: 3  # 断路器设备类型代号
            SA: 1  # 断路器地址
            SID: "4C36210605133128"  # 断路器ID
            GID: "FFD1212006105728"  # 上联网关ID
            OL: true  # 是否在线
        }
    """
    brand = RedisColumn(no_null=True)
    GID = RedisColumn(no_null=True)
    SID = RedisColumn(no_null=True)

    def __init__(self, obj):
        # 处理 SDT1 SA1 SID1
        obj_for_iter = copy.deepcopy(obj)

        for key, value in obj_for_iter.items():
            if re.search('([\d]+)', key):
                new_key = re.search('([^\d]+)', key)[1]
                obj[new_key] = obj[key]
                del obj[key]

        self.obj = obj
        assert (self.validate_obj(obj))

    @property
    def key(self):
        return 'switch_status:%s:%s:%s' % (self.obj['brand'],
                                           self.obj['GID'],
                                           self.obj['SID']
                                           )


class GatewayStatus(RedisModel):
    """
        {
            brand： hongfa/timu
            GDT:23 # 网关设备类型代号
            SCC:2 # 已配置的从设备数量
            SCO:2 # 在线的从设备数量
            GID:"FFD1212006105728" # 网关ID
            OL:true # 是否在线
        }
    """
    brand = RedisColumn(no_null=True)
    GID = RedisColumn(no_null=True)

    @property
    def key(self):
        return 'gateway_status:%s:%s' % (self.obj['brand'], self.obj['GID'])


class SwitchData(RedisModel):
    """
    """
    dtu_sn = RedisColumn(no_null=True)
    date = RedisColumn(no_null=True)
    code = RedisColumn(no_null=True)
    item = RedisColumn(no_null=True)

    @property
    def key(self):
        return 'switch_data:%s:%s' % (self.obj['dtu_sn'], self.obj['item'][0]['cmp_sn1'])


class SwitchTempData(RedisModel):
    """
        {   brand： hongfa/timu
            gateway_sn:"FFD1212006105728"
            switch_sn:"6B02210710091756"
            siblings: 4
            date:1629191317
            data :{
                18004:0
                18005:0
                ...
                }
        }
    """
    brand = RedisColumn(no_null=True)
    gateway_sn = RedisColumn(no_null=True)
    switch_sn = RedisColumn(no_null=True)
    siblings = RedisColumn(no_null=True)
    start_address = RedisColumn(no_null=True)
    date = RedisColumn(no_null=True)
    data = RedisColumn(no_null=True)

    @property
    def key(self):
        # 根据起始寄存器地址判断是不是需要的自动上传数据
        if self.obj['start_address'] not in ['0000', '0016', '002C', '0080']:
            return None
        return 'temp:%s:%s:%s:%s' % (
            self.obj['brand'], self.obj['gateway_sn'], self.obj['switch_sn'], self.obj['start_address'])


class RequestTask(RedisModel):
    """
        入列的网关请求任务
    """
    topic = RedisColumn(no_null=True)

    @property
    def key(self):
        return 'queues:%s' % (self.obj['topic'])
