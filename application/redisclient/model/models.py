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

    @property
    def key(self):
        return 'switch_status:%s:%s:%s' % (self.obj['brand'],
                                             self.obj['GID'],
                                             self.obj['SID'])

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
        {   brand： hongfa/timu
            gateway_id:"FFD1212006105728"
            switch_id:"6B02210710091756"
            date:1629191317
            code:1
            data :{
                18004:0
                18005:0
                ...
                }
        }
    """
    brand = RedisColumn(no_null=True)
    gateway_id = RedisColumn(no_null=True)
    switch_id = RedisColumn(no_null=True)
    date = RedisColumn(no_null=True)
    code = RedisColumn(no_null=True)
    data = RedisColumn(no_null=True)

    @property
    def key(self):
        return 'switch_data:%s:%s:%s' % (self.obj['brand'], self.obj['gateway_id'], self.obj['switch_id'])