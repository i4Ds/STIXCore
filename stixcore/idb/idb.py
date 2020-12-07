import os
import sqlite3
import threading
from types import SimpleNamespace

from stixcore.util.logging import get_logger

thread_lock = threading.Lock()

__all__ = ['IDB', 'IdbData', 'IdbPacketTypeInfo', 'IdbParameter', 'IdbStaticParameter',
           'IdbVariableParameter', 'IdbPacketTree']

logger = get_logger(__name__)


class IdbData(SimpleNamespace):
    """A base class to represent the query results from the IDB."""

    def __init__(self, dbtupel):
        """Construct all the necessary attributes for the IdbData object and stores all
        entries given in the dbtupel internally.

        Parameters
        ----------
        dbtupel : `dict`
            all named parameters from the db query
        """
        self.__dict__.update(dbtupel)


class IdbPacketTypeInfo(IdbData):
    """A class to perpresent descriptive information for a idb packet type.

    Parameters
    ----------
    IdbData : [type]
        [description]
    """
    def __init__(self, dbtupel):
        """Construct all the necessary attributes for the IdbPacketTypeInfo object.

        Parameters
        ----------
        dbtupel : `dict`
            all named parameters from the db query
        """
        super().__init__(dbtupel)

    @property
    def PID_SPID(self):
        """SCOS-2000 Telemetry Packet Number.

        Returns
        -------
        `int`
            Unsigned integer number in the range (1....2^32-1) (note that zero is not allowed).
        """
        return self.__dict__['PID_SPID']

    @property
    def PID_DESCR(self):
        """Textual description of the SCOS-2000 telemetry packet.

        Returns
        -------
        `str`
            max 64 charactars
        """
        return self.__dict__['PID_DESCR']

    @property
    def PID_TPSD(self):
        """SCOS-2000 Telemetry Packet Structure Definition. This field is only used by the
        Variable Packets Display application. It has to be set to ‘-1’ for packets which are
        not defined in the VPD table and thus are not required to be processed by the
        Variable PacketsDisplay.

        Returns
        -------
        `int`
            If not set to –1, unsigned integer number in the range (1....2^31-1)
            (note that zero is not allowed).
        """
        return self.__dict__['PID_TPSD']

    def is_variable(self):
        """Is the telemetry packet of variable length.

        Returns
        -------
        `bool`
            True if the TM packet has a variable size
        """
        return self.PID_TPSD != -1


class IdbParameter(IdbData):
    """A base class to represent a parameter of a SCOS-2000 Telemetry Packet."""

    def __init__(self, dbtupel):
        """Construct all the necessary attributes for the IdbParameter object.

        Parameters
        ----------
        dbtupel : `dict`
            all named parameters from the db query
        """
        super().__init__(dbtupel)

    @property
    def bin_format(self):
        """Read instruction format of the specific parameter for processing the bit stream.
        See `bitstream.ConstBitStream.read`.

        Returns
        -------
        `str`
            The format containing the data type and number of bits to read like "int:8".
        """
        return self._bin_format

    @bin_format.setter
    def bin_format(self, value):
        self._bin_format = value

    @property
    def PID_SPID(self):
        """SCOS-2000 Telemetry Packet Number the parameter belongs to.

        Returns
        -------
        `int`
            Unsigned integer number in the range (1....2^32-1) (note that zero is not allowed).
        """
        return self.__dict__['PID_SPID']

    @property
    def PID_DESCR(self):
        """Textual description of the SCOS-2000 telemetry packet the parameter belongs to.

        Returns
        -------
        `str`
            max 64 charactars
        """
        return self.__dict__['PID_DESCR']

    @property
    def PID_TPSD(self):
        """SCOS-2000 Telemetry Packet Structure Definition. This field is only used by the
        Variable Packets Display application. It has to be set to ‘-1’ for packets which are
        not defined in the VPD table and thus are not required to be processed by the Variable
        PacketsDisplay.

        Returns
        -------
        `int`
            If not set to –1, unsigned integer number in the range (1....2^31-1)
            (note that zero is not allowed).
        """
        return self.__dict__['PID_TPSD']

    @property
    def PCF_DESCR(self):
        """Parameter Description.

        Returns
        -------
        `str`
            Free textual description of the parameter.
        """
        return self.__dict__['PCF_DESCR']

    @property
    def PCF_NAME(self):
        """Name of the parameter. Alphanumeric string uniquely identifying the monitoring
        parameter.

        Returns
        -------
        `str`
            max 8 characters.
        """
        return self.__dict__['PCF_NAME']

    @property
    def PCF_WIDTH(self):
        """'Padded' width of this parameter expressed in number of bits. This field is only
        used when extracting parameter samples using the VPD definition.

        Returns
        -------
        `int`
            to identify the bitposition where the next telemetry parameter starts
        """
        return self.__dict__['PCF_WIDTH']

    @property
    def PCF_PFC(self):
        """Parameter Format Code. Along with the Parameter Type Code (PCF_PTC) this
        field controls the length of the parameter.

        Returns
        -------
        `int`
            Integer value in a range compatible with the specified PCF_PTC
        """
        return self.__dict__['PCF_PFC']

    @property
    def PCF_PTC(self):
        """Parameter Type Code. This controls the encoding format of the parameter.

        Returns
        -------
        `int`
            Integer value in the range (1..13)
        """
        return self.__dict__['PCF_PTC']

    @property
    def PCF_CURTX(self):
        """Parameter calibration identification name.

        Returns
        -------
        `str`
            Depending  on  parameter  category,  this  field  stores  the  numerical
            calibration or the textual calibration identification name.
        """
        return self.__dict__['PCF_CURTX']

    @property
    def S2K_TYPE(self):
        """TBD.

        Returns
        -------
        `str`
            TBD
        """
        return self.__dict__['S2K_TYPE']


class IdbStaticParameter(IdbParameter):
    """A class to represent a parameter of a static SCOS-2000 Telemetry Packet."""

    def __init__(self, dbtupel):
        """Construct all the necessary attributes for the IdbStaticParameter object.

        Parameters
        ----------
        dbtupel : `dict`
            all named parameters from the db query
        """
        super().__init__(dbtupel)

    @property
    def PLF_OFFBY(self):
        """Location of first occurrence of parameter value in octets, relative to the
        end of the SCOS-2000 TM header.

        Returns
        -------
        `int`
            Integer value starting from 0 (negative values are not allowed).
        """
        return self.__dict__['PLF_OFFBY']

    @property
    def PLF_OFFBI(self):
        """Bit number, within an octet, of the first bit of the first occurrence of
        the parameter value. Bit 0 corresponds to the most left bit withinthe byte.

        Returns
        -------
        `int`
            Integer value in the range (0..7).
        """
        return self.__dict__['PLF_OFFBI']

    def is_variable(self):
        """Is the parameter for a variable telemetry packet.

        Returns
        -------
        `bool`
            Always False for this class
        """
        return False


class IdbVariableParameter(IdbParameter):
    """A class to represent a parameter of a variable SCOS-2000 Telemetry Packet."""

    def __init__(self, dbtupel):
        """Construct all the necessary attributes for the IdbVariableParameter object.

        Parameters
        ----------
        dbtupel : `dict`
            all named parameters from the db query
        """
        super().__init__(dbtupel)

    @property
    def VPD_POS(self):
        """Ordinal position of this parameter inside the packet definition

        Returns
        -------
        `int`
            ascending order
        """
        return self.__dict__['VPD_POS']

    @property
    def VPD_OFFSET(self):
        """Number of bits between the start position of this parameter and the end bit of
        the previous parameter in the packet. A positive offsetenables the introduction of
        a ‘gap’ between the previous parameterand this one. A negative offset enables the
        ‘overlap’ of the bitscontributing to this parameter with the ones contributing to
        the previous parameter(s).

        Returns
        -------
        `int`
            Integer value in the range (-32768..32767)
        """
        return self.__dict__['VPD_OFFSET']

    @property
    def VPD_GRPSIZE(self):
        """This value should only be set for parameters which identify a repeat counter.

        Returns
        -------
        `int`
            N repetitions
        """
        return self.__dict__['VPD_GRPSIZE']

    def is_variable(self):
        """Is the parameter for a variable telemetry packet.

        Returns
        -------
        `bool`
            Always True for this class
        """
        return True


class IdbPacketTree():
    """Class representing a dynamic telemetry packet of variable length in a tree structure
    with nested repeaters."""

    def __init__(self, *, children=None, counter=1, name='top', parameter=None):
        """Construct all the necessary attributes for the IdbPacketTree object.

        Parameters
        ----------
        children : `list`, optional
            list of IdbPacketTree, by default None: will be transformed to []
        counter : `int`, optional
            how often this parameter is repeated, by default 1
        name : `str`, optional
            unique name of the parameter, by default 'top'
        parameter : IdbParameter, optional
            enhanced description of the parameter, by default None
        """
        if children is None:
            children = []

        self.children = children
        self.counter = counter
        self.name = name
        self.parameter = parameter

    @property
    def children(self):
        """Sequential ordered list of child Parameters (nested due to repeaters).

        Returns
        -------
        `list`
            List of `~stixcore/idb/idb/IdbPacketTree`
        """
        return self._children

    @children.setter
    def children(self, value):
        self._children = value

    @property
    def parameter(self):
        """Telemetry packet parameter.

        Returns
        -------
        `~stixcore/idb/idb/IdbParameter`
            enhanced description of the parameter
        """
        return self._parameter

    @parameter.setter
    def parameter(self, value):
        self._parameter = value

    @property
    def counter(self):
        """How often this parameter is repeated.

        Returns
        -------
        `int`
            Normally 1 only for repeaters more then 1
        """
        return self._counter

    @counter.setter
    def counter(self, value):
        self._counter = value

    @property
    def name(self):
        """Unique name of the parameter.

        Returns
        -------
        `str`
            Project wide unique name.
        """
        return self._name

    @name.setter
    def name(self, value):
        self._name = value


class IDB:
    """Class provides reading functionality to a IDB (definition of TM/TC packet structures)."""

    def __init__(self, filename):
        """Create the IDB reader for a given file.

        Parameters
        ----------
        filename : `str` | `pathlib.Path`
            Path to the idb file
        """
        self.conn = None
        self.cur = None
        self.parameter_structures = dict()
        self.parameter_units = dict()
        self.calibration_polynomial = dict()
        self.calibration_curves = dict()
        self.textual_parameter_lut = dict()
        self.soc_descriptions = dict()
        self.parameter_descriptions = dict()
        self.s2k_table_contents = dict()
        self.filename = filename
        logger.info(f"Creating IDB reader for: {self.filename}")

        if self.filename:
            self._connect_database()

    def is_connected(self):
        """Is the reader connected to the IDB.

        returns
        -------
        True | False
        """
        if self.cur:
            return True
        return False

    def get_idb_filename(self):
        """Get the path to the connected IDB file.

        returns
        -------
        `os.path`
            the path to the IDB file
        """
        return os.path.abspath(self.filename)

    def _connect_database(self):
        try:
            self.conn = sqlite3.connect(str(self.filename), check_same_thread=False)
            logger.info('IDB loaded from {}'.format(self.filename))
            self.cur = self.conn.cursor()
        except sqlite3.Error:
            logger.error('Failed load IDB from {}'.format(self.filename))
            self.close()
            raise

    def close(self):
        """Close the IDB connection."""
        if self.conn:
            self.conn.close()
            self.cur = None
        else:
            logger.warning("IDB connection already closed")

    def _execute(self, sql, arguments=None, result_type='list'):
        """Execute sql and return results in a list or a dictionary."""
        if not self.cur:
            raise Exception('IDB is not initialized!')
        else:
            rows = None
            try:
                thread_lock.acquire(True)
                # sqlite doesn't like multi-threads

                if arguments:
                    self.cur.execute(sql, arguments)
                else:
                    self.cur.execute(sql)
                if result_type == 'list':
                    rows = self.cur.fetchall()
                else:
                    rows = [
                        dict(
                            zip([column[0]
                                 for column in self.cur.description], row))
                        for row in self.cur.fetchall()
                    ]
            finally:
                thread_lock.release()
            return rows

    def get_spid_info(self, spid):
        """Get SPID description.

        returns
        -------
        (PID_DESCR, PID_TYPE, PID_STYPE)
        """
        sql = 'select PID_DESCR,PID_TYPE,PID_STYPE from PID where PID_SPID=? limit 1'
        return self._execute(sql, (spid, ))

    def get_all_spid(self):
        """get list of all SPIDs and short description

        returns
        -------
        (PID_SPID, PID_DESCR)
        """
        sql = 'select PID_SPID, PID_DESCR from PID'
        return self._execute(sql, None)

    def get_scos_description(self, name):
        """get scos long description

        Parameters
        ----------
        name : ´str´
            the scos_name like 'NIX00354'

        Returns
        -------
        ´str´
            the long description
        """
        if name in self.soc_descriptions:
            return self.soc_descriptions[name]
        else:
            rows = self._execute(
                'select SW_DESCR from sw_para where scos_name=? ', (name, ))
            if rows:
                res = rows[0][0]
                self.soc_descriptions[name] = res
                return res

            logger.warning("nothing found in IDB table: sw_para")
            return ''

    def get_telemetry_description(self, spid):
        """Get telemetry data information.

        Parameters
        ----------
        spid : `int`

        returns
        -------
        (SW_DESCR, tpcf_name)
        """
        sql = ('select sw_para.SW_DESCR, tpcf.tpcf_name  '
               ' from sw_para join tpcf '
               'on tpcf.tpcf_name=sw_para.scos_name and tpcf.tpcf_spid= ?')
        return self._execute(sql, (spid, ))

    def get_packet_type_offset(self, packet_type, packet_subtype):
        """gets (offset, width) for packet type and subtype

        Parameters
        ----------
        packet_type : `int`
        packet_subtype : `int`

        returns
        -------
        (PIC_PI1_OFF, PIC_PI1_WID)
        """
        sql = ('select PIC_PI1_OFF, PIC_PI1_WID from PIC '
               'where PIC_TYPE=? and PIC_STYPE=? limit 1')
        args = (packet_type, packet_subtype)
        rows = self._execute(sql, args)
        if rows:
            return rows[0]

        logger.warning("nothing found in IDB table: PIC")
        return 0, 0

    def get_parameter_description(self, name):
        """get scos long description

        Parameters
        ----------
        name : `str`

        returns
        -------
        ´str´
            a long describtion
        """
        if name in self.parameter_descriptions:
            return self.parameter_descriptions[name]
        else:
            rows = self._execute('select PCF_DESCR from PCF where PCF_NAME=? ',
                                 (name, ))
            if not rows:
                rows = self._execute(
                    'select CPC_DESCR from CPC where CPC_PNAME=? ', (name, ))
            if rows:
                res = rows[0][0]
                self.parameter_descriptions[name] = res
                return res

            logger.warning("nothing found in IDB table: PCF or CPC")
            return ''

    def get_parameter_unit(self, name):
        """get unit for parameter

        Parameters
        ----------
        name : `str`

        returns
        -------
        ´str´
            the unit
        """
        if not self.parameter_units:
            results = self._execute(
                'select PCF_NAME, PCF_UNIT from PCF where PCF_UNIT!=""')
            self.parameter_units = {row[0]: row[1] for row in results}
        if name in self.parameter_units:
            return self.parameter_units[name]

        logger.warning("nothing found in IDB table: PCF")
        return ''

    def get_packet_type_info(self, packet_type, packet_subtype, pi1_val=-1):
        """Identify packet type using service, service subtype and information in IDB table PID

        Parameters
        ----------
        packet_type : `int`
        packet_subtype : `int`
        pi1_val : `int`

        returns
        -------
        `IdbPacketTypeInfo` or `None` if not found
        """
        args = None
        if pi1_val == -1:
            sql = ('select pid_spid, pid_descr, pid_tpsd from PID '
                   'where PID_TYPE=? and PID_STYPE=? limit 1')
            args = (packet_type, packet_subtype)
        else:
            sql = (
                'select pid_spid, pid_descr, pid_tpsd from PID '
                'where PID_TYPE=? and PID_STYPE=? and PID_PI1_VAL=? limit 1')
            args = (packet_type, packet_subtype, pi1_val)
        rows = self._execute(sql, args, 'dict')
        if rows:
            return IdbPacketTypeInfo(rows[0])
        else:
            logger.warning(f"No information in IDB for service {packet_type},"
                           f"service_subtype {packet_subtype}  and pi1_val: {pi1_val}")
            return None

    def get_s2k_parameter_types(self, ptc, pfc):
        """gets parameter type

        Parameters
        ----------
        ptc : `int`
            the paramter
        pfc : `int`
            PFC_LB and PFC_UB

        returns
        -------
        `str`
            the type
        """
        if (ptc, pfc) in self.s2k_table_contents:
            return self.s2k_table_contents[(ptc, pfc)]
        else:
            sql = ('select S2K_TYPE from '
                   ' tblConfigS2KParameterTypes where PTC = ? '
                   ' and ? >= PFC_LB and  PFC_UB >= ? limit 1')
            args = (ptc, pfc, pfc)
            rows = self._execute(sql, args, 'list')
            if rows:
                s2k_type = rows[0][0]
                self.s2k_table_contents[(ptc, pfc)] = s2k_type
                return s2k_type
            logger.warning("nothing found in IDB table: tblConfigS2KParameterTypes")
            return None

    def convert_NIXG_NIXD(self, name):
        """gets NIXG to NIXD  conversation infos for a PDI

        Parameters
        ----------
        name : `str`
            PDI_GLOBAL name

        returns
        -------
        (PDI_GLOBAL, PDI_DETAIL, PDI_OFFSET)
        """
        sql = (
            'select PDI_GLOBAL, PDI_DETAIL, PDI_OFFSET from PDI where PDI_GLOBAL=? '
        )
        args = (name, )
        rows = self._execute(sql, args, 'dict')
        return rows

    def get_fixed_packet_structure(self, spid):
        """get parameter structures using SCO ICD (page 39)

        Parameters
        ----------
        spid: SPID

        returns
        -------
        parameter structures
        """
        if spid in self.parameter_structures:
            return self.parameter_structures[spid]
        sql = (
            'select PCF.PCF_DESCR, PLF.PLF_OFFBY, PLF.PLF_OFFBI, PCF.PCF_NAME,'
            ' PCF.PCF_WIDTH, PCF.PCF_PFC,PCF.PCF_PTC, PCF.PCF_CURTX'
            ' from PLF   inner join PCF  on PLF.PLF_NAME = PCF.PCF_NAME '
            ' and PLF.PLF_SPID=? order by PLF.PLF_OFFBY asc')
        args = (spid, )
        res = self._execute(sql, args, 'dict')
        self.parameter_structures[spid] = res
        return res

    def get_telecommand_info(self, service_type, service_subtype, subtype=None):
        """get TC description for a header

        Parameters
        ----------
        service_type : `int`
        service_subtype : `int`
        subtype : `int`, optional

        returns
        --------
        `dict` | `None`
        """

        sql = (
            'select  CCF_CNAME, CCF_DESCR, CCF_DESCR2, '
            ' CCF_NPARS from CCF where CCF_TYPE=? and CCF_STYPE =? order by CCF_CNAME asc'
        )
        res = self._execute(sql, (service_type, service_subtype), 'dict')
        index = 0
        if len(res) > 1 and (subtype is not None):
            index = subtype - 1
        try:
            return res[index]
        except IndexError:
            logger.warning("nothing found in IDB table: CCF")
            return None

    def get_telecommand_structure(self, name):
        """Get the structure of a telecommand  by its name. The structure will be used to decode
        the TC packet.

        Parameters
        ----------
        name : `str`
            a structure name like 'ZIX06009'

        returns
        -------
        tm structure
        """
        sql = ('select CDF_ELTYPE, CDF_DESCR, CDF_ELLEN, CDF_BIT, '
               'CDF_GRPSIZE, CDF_PNAME, CPC_DESCR,  CPC_PAFREF, CPC_PTC,'
               'CPC_PFC from CDF left join CPC on  CDF_PNAME=CPC_PNAME'
               '  where  CDF_CNAME=?  order by CDF_BIT asc')
        args = (name, )
        res = self._execute(sql, args, 'dict')
        return res

    def is_variable_length_telecommand(self, name):
        """Determines if the TM structure is of variable length

        Parameters
        ----------
        name : `str`
            a structure name like 'ZIX06009'

        returns
        -------
        True|False
        """
        sql = 'select CDF_GRPSIZE  from CDF where CDF_GRPSIZE >0 and CDF_CNAME=?'
        args = (name, )
        rows = self._execute(sql, args, 'list')
        if rows:
            num_repeater = int(rows[0][0])
            if num_repeater > 0:
                return True
        return False

    def get_variable_packet_structure(self, spid):
        """Get the variable packet structure of a telecommand by its spid (VPD.VPD_TPSD).

        Parameters
        ----------
        name : `str`|`int`
            a structure spid like 54118

        returns
        -------
        `list` tm structure
        """
        if spid in self.parameter_structures:
            return self.parameter_structures[spid]
        sql = (
            'select PCF.PCF_NAME, VPD.VPD_POS,PCF.PCF_WIDTH,PCF.PCF_PFC, PCF.PCF_PTC,VPD.'
            'VPD_OFFSET, VPD.VPD_GRPSIZE,PCF.PCF_DESCR ,PCF.PCF_CURTX'
            ' from VPD inner join PCF on  VPD.VPD_NAME=PCF.PCF_NAME and VPD.VPD_TPSD=? order by '
            ' VPD.VPD_POS asc')
        res = self._execute(sql, (spid, ), 'dict')
        self.parameter_structures[spid] = res
        return res

    def tcparam_interpret(self, ref, raw):
        """interpret telecommand parameter by using the table PAS

        Parameters
        ----------
        ref : `str`
            PAS_NUMBR
        raw : `int`
            PAS_ALVAL

        returns
        -------
        `str`
            PAS_ALTXT
        """
        sql = 'select PAS_ALTXT from PAS where PAS_NUMBR=? and PAS_ALVAL=?'
        args = (ref, raw)
        rows = self._execute(sql, args)
        try:
            return rows[0][0]
        except (TypeError, IndexError):
            logger.warning("nothing found in IDB table: PAS")
            return ''
        return ''

    def get_calibration_curve(self, pcf_curtx):
        """calibration curve defined in CAP database

        Parameters
        ----------
        pcf_curtx : `str`
            cap_numbr lile 'CIXP0024TM'

        returns
        -------
        `list`
            calibration curve
        """
        if pcf_curtx in self.calibration_curves:
            return self.calibration_curves[pcf_curtx]
        else:
            sql = ('select cap_xvals, cap_yvals from cap '
                   ' where cap_numbr=? order by cast(CAP_XVALS as double) asc')
            args = (pcf_curtx, )
            rows = self._execute(sql, args)
            self.calibration_curves[pcf_curtx] = rows
            return rows

    def get_textual_mapping(self, parameter_name):
        """get a struct for textual mapping of index and names

        Parameters
        ----------
        parameter_name : `str`
            PCF_NAME lile 'NIX00013'

        returns
        -------
        `array` [(idx,),(name)]
            the mapping
        `None`
            if parameter_name not found
        """
        sql = 'select  TXP_FROM, TXP_ALTXT from TXP join PCF on ' \
              'PCF_CURTX=TXP_NUMBR where PCF_NAME=? order by TXP_FROM asc'
        args = (parameter_name, )
        rows = self._execute(sql, args)
        if rows:
            return ([int(x[0]) for x in rows], [x[1] for x in rows])
        else:
            return None

    def textual_interpret(self, pcf_curtx, raw_value):
        """gets a name for a TXP_NUMBR from TXP for given raw_value

        Parameters
        ----------
        pcf_curtx : `str`
            TXP_NUMBR lile 'CAAT0005TM'
        raw_value : `int`
            value in range of TXP_FROM  to TXP_TO

        returns
        -------
        `list` of `str`
            the names
        """
        if (pcf_curtx, raw_value) in self.textual_parameter_lut:
            # build a lookup table
            return self.textual_parameter_lut[(pcf_curtx, raw_value)]

        sql = ('select TXP_ALTXT from TXP where  TXP_NUMBR=? and ?>=TXP_FROM '
               ' and TXP_TO>=? limit 1')
        args = (pcf_curtx, raw_value, raw_value)
        rows = self._execute(sql, args)
        self.textual_parameter_lut[(pcf_curtx, raw_value)] = rows
        # lookup table
        return rows

    def get_calibration_polynomial(self, mcf_ident):
        """gets calibration polynomial information for a given MCF_IDENT

        Parameters
        ----------
        mcf_ident : `str`
            TXP_NUMBR lile 'CIX00036TM'

        returns
        -------
        (MCF_POL1, MCF_POL2, MCF_POL3, MCF_POL4, MCF_POL5)
        """
        if mcf_ident in self.calibration_polynomial:
            return self.calibration_polynomial[mcf_ident]
        else:
            sql = ('select MCF_POL1, MCF_POL2, MCF_POL3, MCF_POL4, MCF_POL5 '
                   'from MCF where MCF_IDENT=? limit 1')
            args = (mcf_ident, )
            rows = self._execute(sql, args)
            self.calibration_polynomial[mcf_ident] = rows
            return rows

    def get_idb_version(self):
        """gets the verion string of the IDB

        returns
        -------
        `str`
            version lable like "1.1.3"
        """
        try:
            sql = ('select version from IDB limit 1')
            rows = self._execute(sql, None, 'list')
            return rows[0][0]
        except (sqlite3.OperationalError, IndexError):
            logger.warning('No IDB version information found in IDB')
            return '-1'

    @staticmethod
    def _get_stream_type_format(param_type, nbytes):
        """Convert the data type of the IDB into a read instruction format for processing the
        bit stream. See `bitstream.ConstBitStream.read`.

        Parameters
        ----------
        param_type : `str`
            see `~stixcore/idb/idb/IdbParameter.S2K_TYPE`
        nbytes : `int`
            see `~stixcore/idb/idb/IdbParameter.PCF_WIDTH`

        Returns
        -------
        `str`
            The format containing the data type and number of bits to read like "int:8".
        """
        if param_type == 'U':
            return f"uint:{nbytes}"
        elif param_type == 'I' and nbytes <= 6:
            return f"int:{nbytes}"
        elif param_type == 'T':
            raise NotImplementedError("Format Error: to implement: 'T'")
        elif param_type == 'CONTEXT' and nbytes <= 4:
            raise NotImplementedError("Format Error: to implement: 'CONTEXT'")

        raise NotImplementedError(f"Format Error: to implement: '{param_type}:{nbytes}")

    def get_static_structure(self, service_type, service_subtype):
        """Create a static parse struct for the specified TM packet.

        Parameters
        ----------
        service_type : `int`
            The TM packet service type.
        service_subtype : `int`
            The TM packet service subtype.

        Returns
        -------
        `~stixcore/idb/idb/IdbPacketTree`
            In this case the generic IdbPacketTree is flat, but can be used fore
            dynamic parseing anyway.
        """

        sql = ('''SELECT
                    PID_SPID, PID_DESCR, PID_TPSD,
                    PCF.PCF_DESCR, PLF.PLF_OFFBY, PLF.PLF_OFFBI, PCF.PCF_NAME,
                    PCF.PCF_WIDTH, PCF.PCF_PFC,PCF.PCF_PTC, PCF.PCF_CURTX,
                    S2K_TYPE
                FROM PID , PLF, PCF, tblConfigS2KParameterTypes as PTYPE
                WHERE
                    PLF.PLF_NAME = PCF.PCF_NAME
                    AND PID_TYPE = ?
                    AND PID_STYPE = ?
                    AND PLF.PLF_SPID = PID.PID_SPID
                    AND PTYPE.PTC = PCF.PCF_PTC
                    AND PCF.PCF_PFC >= PTYPE.PFC_LB
                    AND PTYPE.PFC_UB >= PCF.PCF_PFC
                ORDER BY
                    PLF.PLF_OFFBY asc ''')
        parameters = self._execute(sql, (service_type, service_subtype), 'dict')

        parent = IdbPacketTree()
        for par in parameters:
            parObj = IdbStaticParameter(par)
            node = self._create_parse_node(parObj.PCF_NAME, parObj, 0, [])
            parent.children.append(node)
        return parent

    def _create_parse_node(self, name, parameter=None, counter=0, children=None):

        if children is None:
            children = []

        parameter.bin_format = self._get_stream_type_format(parameter.S2K_TYPE, parameter.PCF_WIDTH)
        node = IdbPacketTree(name=name, counter=counter, parameter=parameter, children=children)
        return node

    def get_variable_structure(self, service_type, service_subtype, ssid=None):
        """Create a dynamic parse tree for the specified TM packet.

        Parameters
        ----------
        service_type : `int`
            The TM packet service type.
        service_subtype : `int`
            The TM packet service subtype.
        ssid : `int` optional
            The TM packet SSID. Default to `None`

        Returns
        -------
        `~stixcore/idb/idb/IdbPacketTree`
            The IdbPacketTree implements nested repeaters.
        """

        sql = (f'''SELECT
                    PID_SPID, PID_DESCR, PID_TPSD,
                    PCF.PCF_NAME, VPD.VPD_POS,PCF.PCF_WIDTH,PCF.PCF_PFC, PCF.PCF_PTC,VPD.VPD_OFFSET,
                    VPD.VPD_GRPSIZE,PCF.PCF_DESCR ,PCF.PCF_CURTX,
                    S2K_TYPE
                FROM PID , VPD, PCF, tblConfigS2KParameterTypes as PTYPE
                WHERE
                    PID_TYPE = ?
                    AND PID_STYPE = ?
                    {"AND PID_PI1_VAL = ? " if ssid is not None else " "}
                    AND VPD.VPD_NAME = PCF.PCF_NAME
                    AND VPD.VPD_TPSD = PID.PID_SPID
                    AND PTYPE.PTC = PCF.PCF_PTC
                    AND PCF.PCF_PFC >= PTYPE.PFC_LB
                    AND PTYPE.PFC_UB >= PCF.PCF_PFC
                ORDER BY
                    VPD.VPD_POS asc ''')
        args = (service_type, service_subtype)
        if ssid is not None:
            args = args + (ssid,)
        param_pcf_structures = self._execute(sql, args, 'dict')

        repeater = [{'node': IdbPacketTree(), 'counter': 1024}]

        for par in param_pcf_structures:
            parObj = IdbVariableParameter(par)
            if repeater:
                for e in reversed(repeater):
                    e['counter'] -= 1
                    if e['counter'] < 0:
                        repeater.pop()
                        # root will be never popped
            parent = repeater[-1]['node']

            node = self._create_parse_node(parObj.PCF_NAME, parObj, 0, [])
            parent.children.append(node)

            if parObj.VPD_GRPSIZE > 0:
                repeater.append({'node': node, 'counter': parObj.VPD_GRPSIZE})

        return repeater[0]['node']
