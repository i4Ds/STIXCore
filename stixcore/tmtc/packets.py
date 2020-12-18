from stixcore.idb.idb import IDB
from stixcore.idb.manager import IDBManager
from stixcore.time.time import DateTime
from stixcore.tmtc.parser import parse_binary, parse_bitstream, parse_variable

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
    def __init__(self, data, idb):
        """
        Create source packet header

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
    def __init__(self, bitstream, idb):
        """
        Create a TM Data Header

        Parameters
        ----------
        bitstream : `bitstream.ConstBitstream`
        """
        res = parse_bitstream(bitstream, TM_DATA_HEADER_STRUCTURE)
        [setattr(self, key, value)
         for key, value in res['fields'].items() if not key.startswith('spare')]

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
    def __init__(self, bitstream, idb):
        """
        Create a TM Data Header

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

    def __init__(self, data, idb):
        """
        Create a generic packet from the given data.

        Parameters
        ----------
        data : binary or `stixcore.tmtc.SourcePacketHeader`
        """
        if not isinstance(data, SourcePacketHeader):
            data = SourcePacketHeader(data, idb)

        self.source_packet_header = data


class TMPacket(GenericPacket):
    """
    A non-specific TM packet
    """
    def __init__(self, data, idb):
        """
        Create a TMPacket

        Parameters
        ----------
        data : binary or `stixcore.tmtc.packets.SourcePacketHeader`
            Data to create TM packet from
        """
        super().__init__(data, idb)
        self.data_header = TMDataHeader(self.source_packet_header.bitstream, idb)

    @classmethod
    def is_datasource_for(cls, sph):
        return not (sph.process_id == 90 and sph.packet_category == 12)


class TCPacket(GenericPacket):
    """A non-specific TC packet."""

    def __init__(self, data, idb):
        """Create a TCPacket.

        Parameters
        ----------
        data : binary or `stixcore.tmtc.packets.SourcePacketHeader`
            Data to create TC packet from
        """
        super().__init__(data, idb)
        self.data_header = TCDataHeader(self.source_packet_header.bitstream, idb)

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

    def __init__(self, data, idb):
        """Create a new TM packet parsing common source and data headers.

        Parameters
        ----------
        data : binary or `TMPacket`

        idb : `stixcore.idb.idb.IDB` or `stixcore.idb.idb.IDBManager`
            IDB:    The IDB version against the TM packet is parsed.
                    Use that if a specific version should be forced
            IDBManager: the IDB Version will be provided by the manager depending on the packet date
        """
        if not isinstance(data, TMPacket):
            self.source_packet_header = SourcePacketHeader(data, idb)
            self.data_header = TMDataHeader(self.source_packet_header.bitstream, idb)
        else:
            self.source_packet_header = data.source_packet_header
            self.data_header = data.data_header

        self._datetime = DateTime.from_scet(self.data_header.scet_coarse,
                                            self.data_header.scet_fine)
        self._idb_version = None

        _idb = None
        if isinstance(idb, (IDB, IDBManager)):
            if isinstance(idb, IDBManager):
                _idb = idb.get_idb(utc=self.date_time.as_utc())
            else:
                _idb = idb
        else:
            raise ValueError(f'idb must be of instance IDB or IDBManager')

        self._idb_version = _idb.get_idb_version()

        packet_info = _idb.get_packet_type_info(self.service_type, self.service_subtype)
        tree = {}
        if packet_info.is_variable():
            ssid = self.source_packet_header.bitstream.peek('uint:8')
            tree = _idb.get_variable_structure(self.service_type, self.service_subtype, ssid)
        else:
            tree = _idb.get_static_structure(self.service_type, self.service_subtype)

        self.data = parse_variable(self.source_packet_header.bitstream, tree)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.source_packet_header}, {self.data_header})'

    def __str__(self):
        return self.__repr__()

    @property
    def idb_version(self):
        """Get the Version of the IDB against this packet was implemented.

        Returns
        -------
        `str`
            the Version label like '2.3.4' or None
        """
        return self._idb_version

    @property
    def date_time(self):
        """Get the date and time of this TM packet derived from header information

        Returns
        -------
        `stixcore.time.DateTime`
        """
        return self._datetime

    @property
    def service_type(self):
        """Get the TM packet service type from the header.

        Returns
        -------
        `int`
            If the header is not yet available `None`
        """
        return self.data_header.service_type if hasattr(self.data_header, "service_type") else None

    @property
    def service_subtype(self):
        """Get the TM packet service subtype from the header.

        Returns
        -------
        `int`
            If the header is not yet available `None`
        """
        return self.data_header.service_subtype  \
            if hasattr(self.data_header, "service_subtype") else None


class TM_1_1(GenericTMPacket):
    """TM(1,1) Telecommand acceptance report."""

    def __init__(self, data, idb):
        super().__init__(data, idb)
        # structure = _IDB.get_structure(self.service_type, self.service_subtype)
        # data = parse_bitstream(self.source_packet_header.bitstream, structure)
        # self.data = type('PacketData', (), data['fields'])

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 1 and dh.service_subtype == 1


class TM_21_6_30(GenericTMPacket):
    """TM(21, 6) SSID 30 Packet."""

    def __init__(self, data, idb):
        super().__init__(data, idb)
        # ssid = self.source_packet_header.bitstream.peek('uint:8')
        # tree = _IDB.get_variable_structure(self.service_type, self.service_subtype, ssid = ssid)
        # data = parse_variable(self.source_packet_header.bitstream, tree)
        # self.data = type('PacketData', (), data)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6
