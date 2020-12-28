"""Service 21 – Science Data Transfer"""
from stixcore.tmtc.packets import GenericTMPacket


class TM_21_6_20(GenericTMPacket):
    """TM(21, 6) SSID 20: X-ray science data: X-ray data compression \
        level 0 (archive buffer format)."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and dh.pi1_val == 20


class TM_21_6_21(GenericTMPacket):
    """TM(21, 6) SSID 21: X-ray science data: X-ray data compression level 1."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and dh.pi1_val == 21


class TM_21_6_22(GenericTMPacket):
    """TM(21, 6) SSID 22: X-ray science data: X-ray data compression level 2."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and dh.pi1_val == 22


class TM_21_6_23(GenericTMPacket):
    """TM(21, 6) SSID 23: X-ray science data: X-ray data compression level 3 (Visibilities)."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and dh.pi1_val == 23


class TM_21_6_24(GenericTMPacket):
    """TM(21, 6) SSID 24: X-ray science data: X-ray data Spectrogram."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and dh.pi1_val == 24


class TM_21_6_30(GenericTMPacket):
    """TM(21, 6) SSID 30: Quick look data: Summed light curves."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and dh.pi1_val == 30


class TM_21_6_31(GenericTMPacket):
    """TM(21, 6) SSID 31: Quick look data: Background monitor."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and dh.pi1_val == 31


class TM_21_6_32(GenericTMPacket):
    """TM(21, 6) SSID 32: Quick look data: Spectra."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and dh.pi1_val == 32


class TM_21_6_33(GenericTMPacket):
    """TM(21, 6) SSID 33: Quick look data: Variance."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and dh.pi1_val == 33


class TM_21_6_34(GenericTMPacket):
    """TM(21, 6) SSID 34: Quick look data: Flare flag and location."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and dh.pi1_val == 34


class TM_21_6_41(GenericTMPacket):
    """TM(21, 6) SSID 41: Energy Calibration Data."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and dh.pi1_val == 41


class TM_21_6_42(GenericTMPacket):
    """TM(21, 6) SSID 42: Aspect bulk data."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and dh.pi1_val == 42


class TM_21_6_43(GenericTMPacket):
    """TM(21, 6) SSID 43: TM Management status and Flare list."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and dh.pi1_val == 43
