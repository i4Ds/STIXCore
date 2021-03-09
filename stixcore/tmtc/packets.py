from enum import Enum

import numpy as np

from stixcore.datetime.datetime import DateTime
from stixcore.idb.idb import IDB
from stixcore.idb.manager import IDBManager
from stixcore.tmtc.parameter import CompressedParameter, EngineeringParameter, Parameter
from stixcore.tmtc.parser import parse_binary, parse_bitstream, parse_variable

__all__ = ['TMTC', 'SourcePacketHeader', 'TMDataHeader', 'TCDataHeader', 'GenericPacket',
           'TMPacket', 'TCPacket', 'GenericTMPacket']

from stixcore.util.logging import get_logger

logger = get_logger(__name__)

SOURCE_PACKET_HEADER_STRUCTURE = {
    'version': 'uint:3',
    'packet_type': 'uint:1',
    'header_flag': 'uint:1',
    'process_id': 'uint:7',
    'packet_category': 'uint:4',
    'sequence_flag': 'uint:2',
    'sequence_count': 'uint:14',
    'data_length': 'uint:16'
}

TM_DATA_HEADER_STRUCTURE = {
    'spare1': 'uint:1',
    'pus_version': 'uint:3',
    'spare2': 'uint:4',
    'service_type': 'uint:8',
    'service_subtype': 'uint:8',
    'destination_id': 'uint:8',
    'scet_coarse': 'uint:32',
    'scet_fine': 'uint:16'
}

TC_DATA_HEADER_STRUCTURE = {
    'ccsds_flag': 'uint:1',
    'pus_version': 'uint:3',
    'ack_request': 'uint:4',
    'service_type': 'uint:8',
    'service_subtype': 'uint:8',
    'source_id': 'uint:8'
}


class TMTC(Enum):
    All = 3
    TM = 2
    TC = 1


class SourcePacketHeader:
    """
    Source packet header common to all packets.

    Attributes
    ----------
    version : int
       Version number
    packet_type : int
        Packet type
    header_flag : int
        Header flag
    process_id : int
        Process ID or PID
    packet_category : int
        Packet category
    sequence_flag :  int
        Sequence flag (3 stand-alone, 2 last, 1 first, 0 middle)
    sequence_count : int
        Sequence count
    data_length : int
        Data length - 1  in bytes (full packet length data_length + 1 + 6)
    """
    def __init__(self, data):
        """
        Create source packet header.

        Parameters
        ----------
        data : bytes or hex string
            Binary or representation of binary data
        """
        res = parse_binary(data, SOURCE_PACKET_HEADER_STRUCTURE)
        [setattr(self, key, value) for key, value in res['fields'].items()]
        self.bitstream = res['bitstream']

        # if (self.data_length + 7) * 8 != self.bitstream.len:
        #     raise ValueError(f'Source packet header data length: {self.data_length} '
        #                      f'does not match data length {(self.bitstream.len/8)-7}.')

    def __repr__(self):
        param_names_values = [f'{k}={v}' for k, v in self.__dict__.items() if k != 'bitstream']
        return f'{self.__class__.__name__}({", ".join(param_names_values)})'

    def __str__(self):
        return self.__repr__()


class TMDataHeader:
    """
    TM Data Header

    Attributes
    ----------
    pus_version : int
        PUS Version Number
    service_type : int
        Service Type
    service_subtype : int
        Service Subtype
    destination_id : int
        Destination ID
    scet_coarse : int
        SCET coarse time
    scet_fine : int
        SCET fine time
    """
    def __init__(self, bitstream):
        """
        Create a TM Data Header

        Parameters
        ----------
        bitstream : `bitstream.ConstBitstream`
        """
        res = parse_bitstream(bitstream, TM_DATA_HEADER_STRUCTURE)
        [setattr(self, key, value)
            for key, value in res['fields'].items() if not key.startswith('spare')]

        self.datetime = DateTime(coarse=self.scet_coarse, fine=self.scet_fine)

    def __repr__(self):
        param_names_values = [f'{k}={v}' for k, v in self.__dict__.items() if k != 'bitstream']
        return f'{self.__class__.__name__}({", ".join(param_names_values)})'

    def __str__(self):
        return self.__repr__()


class TCDataHeader:
    """
    TM Data Header

    Attributes
    ----------
    ccsds_flag : `int`
        CCSDS secondary header flag
    pus_version : `int`
        PUS version
    ack_request : `int`
        Acknowledgment request flag
    service_type : `int`
        Service Type
    service_subtype : `int`
        Service Subtype
    source_id : `int`
        Source ID
    """
    def __init__(self, bitstream):
        """
        Create a TM Data Header.

        Parameters
        ----------
        bitstream : `bitsream.ConstBitsream`
        """
        res = parse_bitstream(bitstream, TC_DATA_HEADER_STRUCTURE)
        [setattr(self, key, value)
            for key, value in res['fields'].items() if not key.startswith('spare')]

    def __repr__(self):
        param_names_values = [f'{k}={v}' for k, v in self.__dict__.items() if k != 'bitstream']
        return f'{self.__class__.__name__}({", ".join(param_names_values)})'

    def __str__(self):
        return self.__repr__()


class GenericPacket:
    """
    Generic TM/TC packer class

    Attributes
    ----------
    _registry : dict
        Dictionary mapping classes (key) to function (value) which validates input.

    """
    _registry = dict()
    idb_manager = None

    def __init_subclass__(cls, **kwargs):
        """
        An __init_subclass__ hook initializes all of the subclasses of a given class.
        So for each subclass, it will call this block of code on import.
        This registers each subclass in a dict that has the `is_datasource_for` attribute.
        This is then passed into the factory so we can register them.
        """
        super().__init_subclass__(**kwargs)
        if hasattr(cls, 'is_datasource_for'):
            cls._registry[cls] = cls.is_datasource_for

    def __init__(self, data):
        """
        Create a generic packet from the given data.

        Parameters
        ----------
        data : binary or `stixcore.tmtc.SourcePacketHeader`
        """
        if not isinstance(data, SourcePacketHeader):
            data = SourcePacketHeader(data)

        self.source_packet_header = data


class TMPacket(GenericPacket):
    """
    A non-specific TM packet

    """
    def __init__(self, data, idb=None):
        """
        Create a TMPacket

        Parameters
        ----------
        data : binary or `stixcore.tmtc.packets.SourcePacketHeader`
            Data to create TM packet from
        """
        if isinstance(data, TMPacket):
            self.source_packet_header = data.source.source_packet_header
            self.data_header = data.data_header
            self._pi1_val = data.pi1_val
        else:
            super().__init__(data)
            self.data_header = TMDataHeader(self.source_packet_header.bitstream)
            self._pi1_val = False

        self.idb = idb
        if not idb:
            self.idb = self.idb_manager.get_idb(obt=self.data_header.datetime)

    @property
    def key(self):
        key = (self.data_header.service_type, self.data_header.service_subtype)
        if self.pi1_val is not None:
            key = key + (self.pi1_val, )
        return key

    @property
    def pi1_val(self):
        if self._pi1_val is not False:
            return self._pi1_val

        pi1_pos = self.idb.get_packet_pi1_val_position(self.data_header.service_type,
                                                       self.data_header.service_subtype)
        if pi1_pos:
            read_format = f"pad:{pi1_pos.offset}, uint:{pi1_pos.width}"
            self._pi1_val = self.source_packet_header.bitstream.peeklist(read_format)[-1]
        else:
            self._pi1_val = None

        return self._pi1_val

    @classmethod
    def is_datasource_for(cls, sph):
        return not (sph.process_id == 90 and sph.packet_category == 12)


class TCPacket(GenericPacket):
    """A non-specific TC packet."""

    def __init__(self, data):
        """Create a TCPacket.

        Parameters
        ----------
        data : binary or `stixcore.tmtc.packets.SourcePacketHeader`
            Data to create TC packet from
        """
        super().__init__(data)
        self.data_header = TCDataHeader(self.source_packet_header.bitstream)

    @classmethod
    def is_datasource_for(cls, sph):
        return sph.packet_category == 12 and sph.process_id == 90


class GenericTMPacket:
    """Generic TM packet all specific TM packets are subclasses of this class.

    Attributes
    ----------
    _registry : `dict`
        Dictionary mapping classes (key) to function (value) which validates input.

    Parameters
    ----------
    data : binary data or `stixcore.tmtc.packets.TMPacket`
        Data to create TM packet from
    """

    # TODO move that struct(s) to a global configuration?
    _SKM_GROUPS = {
            'EACC':     ("NIXD0007", "NIXD0008", "NIXD0009"),
            'VIS':      (1,          "NIXD0008", "NIXD0009"),  # hard coded s=1 for negative values
            'ETRIG':    ("NIXD0010", "NIXD0011", "NIXD0012"),
            'LC':       ("NIXD0101", "NIXD0102", "NIXD0103"),
            'TriggerSSID30': ("NIXD0104", "NIXD0105", "NIXD0106"),
            'BKG':      ("NIXD0108", "NIXD0109", "NIXD0110"),
            'TRIG':     ("NIXD0112", "NIXD0113", "NIXD0114"),
            'SPEC':     ("NIXD0115", "NIXD0116", "NIXD0117"),
            'VAR':      ("NIXD0118", "NIXD0119", "NIXD0120"),
            'CALI':     ("NIXD0126", "NIXD0127", "NIXD0128")
    }

    _SCHEMAS = {
                21: {
                        'NIX00260': _SKM_GROUPS['EACC'],
                        'NIX00242': _SKM_GROUPS['ETRIG'],
                        'NIX00243': _SKM_GROUPS['ETRIG'],
                        'NIX00244': _SKM_GROUPS['ETRIG'],
                        'NIX00245': _SKM_GROUPS['ETRIG'],
                        'NIX00246': _SKM_GROUPS['ETRIG'],
                        'NIX00247': _SKM_GROUPS['ETRIG'],
                        'NIX00248': _SKM_GROUPS['ETRIG'],
                        'NIX00249': _SKM_GROUPS['ETRIG'],
                        'NIX00250': _SKM_GROUPS['ETRIG'],
                        'NIX00251': _SKM_GROUPS['ETRIG'],
                        'NIX00252': _SKM_GROUPS['ETRIG'],
                        'NIX00253': _SKM_GROUPS['ETRIG'],
                        'NIX00254': _SKM_GROUPS['ETRIG'],
                        'NIX00255': _SKM_GROUPS['ETRIG'],
                        'NIX00256': _SKM_GROUPS['ETRIG'],
                        'NIX00257': _SKM_GROUPS['ETRIG']
                    },
                22: {
                        'NIX00260': _SKM_GROUPS['EACC'],
                        'NIX00242': _SKM_GROUPS['ETRIG'],
                        'NIX00243': _SKM_GROUPS['ETRIG'],
                        'NIX00244': _SKM_GROUPS['ETRIG'],
                        'NIX00245': _SKM_GROUPS['ETRIG'],
                        'NIX00246': _SKM_GROUPS['ETRIG'],
                        'NIX00247': _SKM_GROUPS['ETRIG'],
                        'NIX00248': _SKM_GROUPS['ETRIG'],
                        'NIX00249': _SKM_GROUPS['ETRIG'],
                        'NIX00250': _SKM_GROUPS['ETRIG'],
                        'NIX00251': _SKM_GROUPS['ETRIG'],
                        'NIX00252': _SKM_GROUPS['ETRIG'],
                        'NIX00253': _SKM_GROUPS['ETRIG'],
                        'NIX00254': _SKM_GROUPS['ETRIG'],
                        'NIX00255': _SKM_GROUPS['ETRIG'],
                        'NIX00256': _SKM_GROUPS['ETRIG'],
                        'NIX00257': _SKM_GROUPS['ETRIG']
                    },
                23: {
                        'NIX00263': _SKM_GROUPS['VIS'],
                        'NIX00264': _SKM_GROUPS['VIS'],
                        'NIX00261': _SKM_GROUPS['EACC'],
                        'NIX00242': _SKM_GROUPS['ETRIG'],
                        'NIX00243': _SKM_GROUPS['ETRIG'],
                        'NIX00244': _SKM_GROUPS['ETRIG'],
                        'NIX00245': _SKM_GROUPS['ETRIG'],
                        'NIX00246': _SKM_GROUPS['ETRIG'],
                        'NIX00247': _SKM_GROUPS['ETRIG'],
                        'NIX00248': _SKM_GROUPS['ETRIG'],
                        'NIX00249': _SKM_GROUPS['ETRIG'],
                        'NIX00250': _SKM_GROUPS['ETRIG'],
                        'NIX00251': _SKM_GROUPS['ETRIG'],
                        'NIX00252': _SKM_GROUPS['ETRIG'],
                        'NIX00253': _SKM_GROUPS['ETRIG'],
                        'NIX00254': _SKM_GROUPS['ETRIG'],
                        'NIX00255': _SKM_GROUPS['ETRIG'],
                        'NIX00256': _SKM_GROUPS['ETRIG'],
                        'NIX00257': _SKM_GROUPS['ETRIG']
                    },
                24: {
                        'NIX00268': _SKM_GROUPS['EACC'],
                        'NIX00267': _SKM_GROUPS['ETRIG']
                    },
                30: {
                        'NIX00272': _SKM_GROUPS['LC'],
                        'NIX00274': _SKM_GROUPS['TriggerSSID30']
                    },
                31: {
                        'NIX00278': _SKM_GROUPS['BKG'],
                        'NIX00274': _SKM_GROUPS['TRIG']
                    },
                32: {
                        'NIX00452': _SKM_GROUPS['SPEC'],
                        'NIX00453': _SKM_GROUPS['SPEC'],
                        'NIX00454': _SKM_GROUPS['SPEC'],
                        'NIX00455': _SKM_GROUPS['SPEC'],
                        'NIX00456': _SKM_GROUPS['SPEC'],
                        'NIX00457': _SKM_GROUPS['SPEC'],
                        'NIX00458': _SKM_GROUPS['SPEC'],
                        'NIX00459': _SKM_GROUPS['SPEC'],
                        'NIX00460': _SKM_GROUPS['SPEC'],
                        'NIX00461': _SKM_GROUPS['SPEC'],
                        'NIX00462': _SKM_GROUPS['SPEC'],
                        'NIX00463': _SKM_GROUPS['SPEC'],
                        'NIX00464': _SKM_GROUPS['SPEC'],
                        'NIX00465': _SKM_GROUPS['SPEC'],
                        'NIX00466': _SKM_GROUPS['SPEC'],
                        'NIX00467': _SKM_GROUPS['SPEC'],
                        'NIX00468': _SKM_GROUPS['SPEC'],
                        'NIX00469': _SKM_GROUPS['SPEC'],
                        'NIX00470': _SKM_GROUPS['SPEC'],
                        'NIX00471': _SKM_GROUPS['SPEC'],
                        'NIX00472': _SKM_GROUPS['SPEC'],
                        'NIX00473': _SKM_GROUPS['SPEC'],
                        'NIX00474': _SKM_GROUPS['SPEC'],
                        'NIX00475': _SKM_GROUPS['SPEC'],
                        'NIX00476': _SKM_GROUPS['SPEC'],
                        'NIX00477': _SKM_GROUPS['SPEC'],
                        'NIX00478': _SKM_GROUPS['SPEC'],
                        'NIX00479': _SKM_GROUPS['SPEC'],
                        'NIX00480': _SKM_GROUPS['SPEC'],
                        'NIX00481': _SKM_GROUPS['SPEC'],
                        'NIX00482': _SKM_GROUPS['SPEC'],
                        'NIX00483': _SKM_GROUPS['SPEC'],
                        'NIX00484': _SKM_GROUPS['TRIG']
                    },
                33: {
                        'NIX00281': _SKM_GROUPS['VAR']
                    },
                41: {
                        'NIX00158': _SKM_GROUPS['CALI']
                    }
    }

    _registry = dict()

    def __init_subclass__(cls, **kwargs):
        """__init_subclass__ hook initializes all of the subclasses of a given class.

        So for each subclass, it will call this block of code on import.
        This registers each subclass in a dict that has the `is_datasource_for` attribute.
        This is then passed into the factory so we can register them.
        """
        super().__init_subclass__(**kwargs)
        if hasattr(cls, 'is_datasource_for'):
            cls._registry[cls] = cls.is_datasource_for

    def __init__(self, data, idb=None):
        """Create a new TM packet parsing common source and data headers.

        Parameters
        ----------
        data : binary or `TMPacket`
        """
        if not isinstance(data, TMPacket):
            # TODO should just create TMPacket here
            data = TMPacket(data, idb=idb)

        self.source_packet_header = data.source_packet_header
        self.data_header = data.data_header
        self.idb = data.idb_manager if idb is None else idb
        self.pi1_val = getattr(data, 'pi1_val', None)

        if isinstance(self.idb, IDBManager):
            idb = self.idb.get_idb(obt=self.data_header.datetime)

        packet_info = idb.get_packet_type_info(self.data_header.service_type,
                                               self.data_header.service_subtype,
                                               self.pi1_val)
        self.spid = packet_info.PID_SPID

        tree = {}
        if packet_info.is_variable():
            tree = idb.get_variable_structure(self.data_header.service_type,
                                              self.data_header.service_subtype,
                                              self.pi1_val)
        else:
            tree = idb.get_static_structure(self.data_header.service_type,
                                            self.data_header.service_subtype,
                                            self.pi1_val)

        self.data = parse_variable(self.source_packet_header.bitstream, tree)
        # self.group_repeaters()

    def group_repeaters(self):
        """Combine the flattend data array of nested repeaters into a nested list.

        Has to be overridden by individual TM packets with nested repeaters.
        """

    def get(self, name):
        try:
            if name in SOURCE_PACKET_HEADER_STRUCTURE.keys():
                return self.source_headers.__getattribute__(name)
            elif name in TM_DATA_HEADER_STRUCTURE.keys():
                return self.data_headers.__getattribute__(name)
            else:
                return self.data.__getattribute__(name)
        except KeyError:
            logger.debug('Key %s not found', name)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.source_packet_header}, {self.data_header})'

    def __str__(self):
        return self.__repr__()

    def get_idb(self):
        idb = None
        if isinstance(self.idb, IDBManager):
            idb = self.idb.get_idb(obt=self.data_header.datetime)
        if isinstance(self.idb, IDB):
            idb = self.idb
        return idb

    def get_calibration_params(self):
        idb = self.get_idb()
        if idb is not None:
            return idb.get_params_for_calibration(self.data_header.service_type,
                                                  self.data_header.service_subtype,
                                                  self.pi1_val)
        return []

    # @property
    # def idb_version(self):
    #     """Get the Version of the IDB against this packet was implemented.
    #
    #     Returns
    #     -------
    #     `str`
    #         the Version label like '2.3.4' or None
    #     """
    #     return self._idb_version

    def get_decompression_parameter(self):
        """List of parameter names that should be decompressed.

        The corresponding decompressions setting parameters are also attached.

        Returns
        -------
        `dict` or None
            {'NIX00472': ("NIXD0007", "NIXD0008", "NIXD0009"), ...}
        """
        if self.data_header.service_type == 21 and self.data_header.service_subtype == 6:
            if self.pi1_val in self._SCHEMAS:
                return self._SCHEMAS[self.pi1_val]
        return None


class PacketSequence:
    """
    A sequence of packets
    """
    def __init__(self, packets):

        self.source_headers = []
        self.data_headers = []
        self.data = []
        self.spid = []
        self.pi1_val = []
        for packet in packets:
            self.spid.append(packet.spid)
            self.pi1_val.append(packet.pi1_val)
            self.source_headers.append(packet.source_packet_header)
            self.data_headers.append(packet.data_header)
            self.data.append(packet.data)

    def get(self, name):
        try:
            if name in {'spid', 'pi1_val'}:
                return self.__getattribute__(name)
            elif name in SOURCE_PACKET_HEADER_STRUCTURE.keys():
                return [header.__getattribute__(name) for header in self.source_headers]
            elif name in TM_DATA_HEADER_STRUCTURE.keys():
                return [header.__getattribute__(name) for header in self.data_headers]
            else:
                return [data.__getattribute__(name) for data in self.data]
        except KeyError:
            logger.debug('Key %s not found', name)

    def get_value(self, name, attr=None):
        if attr is None:
            if isinstance(self.data[0].__getattribute__(name), EngineeringParameter):
                attr = 'engineering'
            elif isinstance(self.data[0].__getattribute__(name), CompressedParameter):
                attr = 'decompressed'
            elif isinstance(self.data[0].__getattribute__(name), Parameter):
                attr = 'value'
            else:
                return [data.__getattribute__(name) for data in self.data]

        res = []
        for d in self.data:
            p = d.__getattribute__(name).__getattribute__(attr)
            if isinstance(p, list):
                res.extend(p)
            else:
                res.append(p)
        return np.hstack(res)
