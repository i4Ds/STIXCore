from stixcore.tmtc.parser import parse_binary, parse_bitstream, parse_repeated

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
    def __init__(self, data):
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
    def __init__(self, bitstream):
        """
        Create a TM Data Header

        Parameters
        ----------
        bitstream : `bitsream.ConstBitsream`
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
    def __init__(self, bitstream):
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
    def __init__(self, data):
        """
        Create a TMPacket

        Parameters
        ----------
        data : binary or `stixcore.tmtc.packets.SourcePacketHeader`
            Data to create TM packet from
        """
        super().__init__(data)
        self.data_header = TMDataHeader(self.source_packet_header.bitstream)

    @classmethod
    def is_datasource_for(cls, sph):
        return not (sph.process_id == 90 and sph.packet_category == 12)


class TCPacket(GenericPacket):
    """
    A non-specific TC packet
    """
    def __init__(self, data):
        """
        Create a TCPacket

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
    """
    A generic TM packet all specific TM packets are subclasses of this class.

    Attributes
    ----------
    _registry : `dict`
        Dictionary mapping classes (key) to function (value) which validates input.

    Parameters
    ----------
    data : binary data or `stixcore.tmtc.packets.TMPacket`
        Data to create TM packet from

    Notes
    -----
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

    def __init__(self, data):
        """
        Create a new TM packet parsing common source and data headers

        Parameters
        ----------
        data : binary or `TMPacket`
        """
        if not isinstance(data, TMPacket):
            self.source_packet_header = SourcePacketHeader(data)
            self.data_header = TMDataHeader(self.source_packet_header.bitstream)
        else:
            self.source_packet_header = data.source_packet_header
            self.data_header = data.data_header

    def __repr__(self):
        return f'{self.__class__.__name__}({self.source_packet_header}, {self.data_header})'

    def __str__(self):
        return self.__repr__()


class TM_1_1(GenericTMPacket):
    """
    TM(1,1) Telecommand acceptance report
    """
    def __init__(self, data):
        super().__init__(data)
        structure = {'NIX00001': 'uint:16', 'NIX00002': 'uint:16'}
        res = parse_bitstream(self.source_packet_header.bitstream, structure)
        [setattr(self, key, value) for key, value in res['fields'].items()]

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 1 and dh.service_subtype == 1


class TM_21_6_30(GenericTMPacket):
    """
    TM(21, 6) SSID 30
    """
    def __init__(self, data):
        super().__init__(data)
        attr_dict = dict()
        fixed_structure = {
            'NIX00120': 'uint:8',
            'NIX00445': 'uint:32',
            'NIX00446': 'uint:16',
            'NIX00405': 'uint:16',
            'NIX00407': 'uint:32',
            'spare1': 'uint:4',
            'NIXD0407': 'uint:12',
            'spare2': 'uint:1',
            'NIXD0101': 'uint:1',
            'NIXD0102': 'uint:3',
            'NIXD0103': 'uint:3',
            'NIXD0104': 'uint:1',
            'NIXD0105': 'uint:3',
            'NIXD0106': 'uint:3',
            'NIXD0107': 'uint:1',
            'NIX00266': 'uint:32',
            'NIX00270': 'uint:8'
        }
        fixed = parse_bitstream(self.source_packet_header.bitstream, fixed_structure)
        attr_dict.update(fixed['fields'])
        num_energies = fixed['fields']['NIX00270']
        # Peak ahead but don't advance pos index
        num_data_points = self.source_packet_header.bitstream.peek('uint:16')
        repeaters_structures = [({'NIX00271': ', '.join(['uint:16']),
                                  'NIX00272': ', '.join(['uint:8']*num_data_points)}, num_energies),
                                ({'NIX00273': 'uint:16'}, 1),
                                ({'NIX00274': ', '.join(['uint:8']*num_data_points)}, 1),
                                ({'NIX00275': 'uint:16'}, 1),
                                ({'NIX00276': ', '.join(['uint:8']*num_data_points)}, 1)]

        for i, (structure, repeats) in enumerate(repeaters_structures):
            dynamic = parse_repeated(self.source_packet_header.bitstream, structure, repeats)
            attr_dict.update(dynamic['fields'])

        self.data = type('PacketData', (), attr_dict)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 21 and dh.service_subtype == 6
