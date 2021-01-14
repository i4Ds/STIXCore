"""Service 21 – Science Data Transfer"""
from stixcore.tmtc.packets import GenericTMPacket
from stixcore.tmtc.parser import split_into_length


class TM_21_6_20(GenericTMPacket):
    """TM(21, 6) SSID 20: X-ray science data: X-ray data compression \
        level 0 (archive buffer format)."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 20


class TM_21_6_21(GenericTMPacket):
    """TM(21, 6) SSID 21: X-ray science data: X-ray data compression level 1."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 21

    def unflatten(self):
        for subpacket in self.data.get_subpackets():
            subpacket.NIX00260 = split_into_length(subpacket.NIX00260, subpacket.NIX00259)

    def get_decompression_parameter(self):
        # https://github.com/i4Ds/STIX-FSW/issues/953
        params = super().get_decompression_parameter()
        if self.data_header.date_time:
            # TODO do some special treatment
            return params
        return params


class TM_21_6_22(GenericTMPacket):
    """TM(21, 6) SSID 22: X-ray science data: X-ray data compression level 2."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 22

    def unflatten(self):
        for subpacket in self.data.get_subpackets():
            subpacket.NIX00260 = split_into_length(subpacket.NIX00260, subpacket.NIX00259)


class TM_21_6_23(GenericTMPacket):
    """TM(21, 6) SSID 23: X-ray science data: X-ray data compression level 3 (Visibilities)."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 23

    def unflatten(self):
        for subpacket in self.data.get_subpackets():
            subpacket.NIX00100 = split_into_length(subpacket.NIX00100, subpacket.NIX00262)
            subpacket.NIX00263 = split_into_length(subpacket.NIX00263, subpacket.NIX00262)
            subpacket.NIX00264 = split_into_length(subpacket.NIX00264, subpacket.NIX00262)


class TM_21_6_24(GenericTMPacket):
    """TM(21, 6) SSID 24: X-ray science data: X-ray data Spectrogram."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 24

    def unflatten(self):
        for subpacket in self.data.get_subpackets():
            subpacket.NIX00268 = split_into_length(subpacket.NIX00268, subpacket.NIX00270)


class TM_21_6_30(GenericTMPacket):
    """TM(21, 6) SSID 30: Quick look data: Summed light curves."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 30

    def unflatten(self):
        self.data.NIX00272 = split_into_length(self.data.NIX00272, self.data.NIX00271)


class TM_21_6_31(GenericTMPacket):
    """TM(21, 6) SSID 31: Quick look data: Background monitor."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 31

    def unflatten(self):
        self.data.NIX00278 = split_into_length(self.data.NIX00278, self.data.NIX00277)


class TM_21_6_32(GenericTMPacket):
    """TM(21, 6) SSID 32: Quick look data: Spectra."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 32


class TM_21_6_33(GenericTMPacket):
    """TM(21, 6) SSID 33: Quick look data: Variance."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 33


class TM_21_6_34(GenericTMPacket):
    """TM(21, 6) SSID 34: Quick look data: Flare flag and location."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 34


class TM_21_6_41(GenericTMPacket):
    """TM(21, 6) SSID 41: Energy Calibration Data."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 41

    def unflatten(self):
        self.data.NIX00158 = split_into_length(self.data.NIX00158, self.data.NIX00146)


class TM_21_6_42(GenericTMPacket):
    """TM(21, 6) SSID 42: Aspect bulk data."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 42


class TM_21_6_43(GenericTMPacket):
    """TM(21, 6) SSID 43: TM Management status and Flare list."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 43
