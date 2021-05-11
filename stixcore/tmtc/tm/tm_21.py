"""Service 21 – Science Data Transfer"""
from copy import copy

import numpy as np

from stixcore.tmtc.packets import GenericTMPacket

__all__ = ['TM_21_6_20', 'TM_21_6_21', 'TM_21_6_22', 'TM_21_6_23', 'TM_21_6_24', 'TM_21_6_30',
           'TM_21_6_31', 'TM_21_6_32', 'TM_21_6_33', 'TM_21_6_34', 'TM_21_6_41', 'TM_21_6_42',
           'TM_21_6_43']


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

    def group_repeaters(self):
        for subpacket in self.data.get_subpackets():
            grouped = np.hstack([p.value for p in subpacket.NIX00260])
            param = subpacket.NIX00260[0]
            param.value = grouped
            subpacket.NIX00260 = param

    def get_decompression_parameter(self):
        # https://github.com/i4Ds/STIX-FSW/issues/953
        params = super().get_decompression_parameter()
        if self.data_header.datetime:
            # TODO do some special treatment
            return params
        return params


class TM_21_6_22(GenericTMPacket):
    """TM(21, 6) SSID 22: X-ray science data: X-ray data compression level 2."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 22

    def group_repeaters(self):
        for subpacket in self.data.get_subpackets():
            p = subpacket.NIX00260[0]
            p.value = np.hstack([p.value for p in subpacket.NIX00260]).squeeze()
            subpacket.NIX00260 = p


class TM_21_6_23(GenericTMPacket):
    """TM(21, 6) SSID 23: X-ray science data: X-ray data compression level 3 (Visibilities)."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 23

    def group_repeaters(self):
        for subpacket in self.data.get_subpackets():
            for nix in ['NIX00100', 'NIX00263', 'NIX00264']:
                p = getattr(subpacket, nix)[0]
                p.value = np.hstack([p.value for p in getattr(subpacket, nix)]).squeeze()
                setattr(subpacket, nix, p)

    def get_decompression_parameter(self):
        # See https://github.com/i4Ds/STIX-FSW/pull/970  flux and vis share the same
        # compression parameters but this is not idea as flux can be considerably larger so it was
        # decided to change this onboard
        # TODO should only be active after FSW v180
        params = copy(super(TM_21_6_23, self).get_decompression_parameter())
        skm_nixs = params['NIX00261']
        s, k, m = [self.data.get(nix).value for nix in skm_nixs]
        params['NIX00261'] = (s-1, k+1, m)
        return params


class TM_21_6_24(GenericTMPacket):
    """TM(21, 6) SSID 24: X-ray science data: X-ray data Spectrogram."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 24

    def group_repeaters(self):
        for subpacket in self.data.get_subpackets():
            p = subpacket.NIX00268[0]
            p.value = np.hstack([p.value for p in subpacket.NIX00268]).squeeze()
            subpacket.NIX00268 = p


class TM_21_6_30(GenericTMPacket):
    """TM(21, 6) SSID 30: Quick look data: Summed light curves."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 30

    def group_repeaters(self):
        grouped = self.data.NIX00272[0]
        grouped.value = np.array([p.value for p in self.data.NIX00272]).squeeze()
        self.data.NIX00272 = grouped


class TM_21_6_31(GenericTMPacket):
    """TM(21, 6) SSID 31: Quick look data: Background monitor."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6 and tm_packet.pi1_val == 31

    def group_repeaters(self):
        grouped = self.data.NIX00278[0]
        grouped.value = np.array([p.value for p in self.data.NIX00278]).squeeze()
        self.data.NIX00282 = grouped


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

    def group_repeaters(self):
        grouped = self.data.NIX00158[0]
        grouped.value = np.array([p.value for p in self.data.NIX00158]).squeeze()
        self.data.NIX00158 = grouped


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
