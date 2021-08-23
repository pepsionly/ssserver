import re
import struct

class HexConverter:

    @staticmethod
    def hex_to_utinyint(s):
        """
        :param s: '1C'
        :return: 28
        """
        s = "%04X" % int(s)
        return HexConverter.hex_to_ushort(s)

    @staticmethod
    def hex_to_hex(s):
        """
        他读到是啥就是啥,不用转换
        :param s: 啥
        :return: 啥
        """
        # 补位
        s = str(s)
        while len(s) < 4:
            s = '0' + s
        return s.upper()

    @staticmethod
    def hex_to_float(s):
        """
        :param s: '435A91AA'
        :return: 218.56900024414062
        """
        return struct.unpack('!f', bytes.fromhex(s))[0]

    @staticmethod
    def float_to_hex(f):
        """
        :param f: 218.56900024414062
        :return: '435A91AA'
        """
        # print(struct.pack('<f', f).hex())  # 小端
        # 输出：32333430
        # print(struct.pack('>f', f).hex())  # 大端
        # 输出：30343332
        return struct.pack('!f', f).hex().upper()

    @staticmethod
    def hex_to_short(s):
        """
        :param s: '001C'
        :return: 28
        """
        return struct.unpack('!h', bytes.fromhex(s))[0]

    @staticmethod
    def short_to_hex(s):
        """
        :param s: 28
        :return: '001C'
        """
        return struct.pack('!h', s).hex().upper()

    @staticmethod
    def hex_to_int(s):
        """
        :param s: 'f09f8fb3'
        :return: -257978445
        """
        return struct.unpack('!i', bytes.fromhex(s))[0]

    @staticmethod
    def int_to_hex(i):
        """
        :param i: -257978445
        :return: 'f09f8fb3'
        """
        return struct.pack('!i', i).hex().upper()

    @staticmethod
    def hex_to_ushort(s):
        """
        :param s: '1386'
        :return: 4998
        """
        return struct.unpack('!H', bytes.fromhex(s))[0]

    @staticmethod
    def ushort_to_hex(s):
        """
        :param s: 4998
        :return: '1386'
        """
        return struct.pack('!H', int(s)).hex().upper()

    @staticmethod
    def hex_to_uint(s):
        """
        :param s: 'efb88fe2'
        :return: 4021850082
        """
        return struct.unpack('!I', bytes.fromhex(s))[0]

    @staticmethod
    def uint_to_hex(i):
        """
        :param i: 4036988851
        :return: 'f09f8fb3'
        """
        return struct.pack('!I', i).hex().upper()

    @staticmethod
    def hex_to_ustr(s):
        """
        无符号字符型字符串转string
        :param s: '615a'
        :return: 'aZ'
        """
        char_str = ''
        s = re.findall(r'.{2}', s)
        for item in s:
            char_str += chr(int(item, 16))
        return char_str

    @staticmethod
    def ustr_to_hex(s):
        """
        string转无符号字符型字符串
        :param s: 'aZ'
        :return: '615a'
        """
        hex_str = ''
        for i in range(len(s)):
            hex_str += struct.pack('B', ord(s[i])).hex()
        return hex_str.upper()

    @staticmethod
    def twosComplement_hex(hexval, bits=16):
        """
        二进制补码算法，16进制有符号整形转换成
        :param hexval: f09f0000 or 0xf09f0000
        :param bits: 多少个bit
        :return: int
        """
        val = int(hexval, 16)
        if val & (1 << (bits - 1)):
            val -= 1 << bits
        return val.upper()

