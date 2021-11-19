import os
import sys
import sqlite3
import threading
from types import SimpleNamespace

import numpy as np
from scipy import interpolate

from stixcore.util.logging import get_logger

__all__ = ['IDB', 'IDBPacketTypeInfo', 'IDBParameter', 'IDBStaticParameter',
           'IDBVariableParameter', 'IDBPacketTree', 'IDBPi1ValPosition',
           'IDBPolynomialCalibration', 'IDBCalibrationCurve', 'IDBCalibrationParameter']

logger = get_logger(__name__)

lock = threading.Lock()


class IDBPi1ValPosition(SimpleNamespace):
    """A class to represent parsing information for optional PI1_Val identifier.

    Attributes
    ----------
    PIC_PI1_OFF : `int`
       PIC_PI1_OFF

    PIC_PI1_WID : `int`
        PIC_PI1_WID
    """
    def __init__(self, *, PIC_PI1_OFF, PIC_PI1_WID):
        super().__init__(PIC_PI1_OFF=PIC_PI1_OFF, PIC_PI1_WID=PIC_PI1_WID)

    @property
    def offset(self):
        """Get number of bits as start position for the PI1_Val parameter started after header.

        Derived from PIC_PI1_OFF

        Returns
        -------
        `int`
            Unsigned integer number of bits
        """
        return (int(self.PIC_PI1_OFF) - 16) * 8

    @property
    def width(self):
        """Get number of bits to read for the PI1_Val parameter.

        Derived from PIC_PI1_WID

        Returns
        -------
        `int`
            bits
        """
        return self.PIC_PI1_WID


class IDBPacketTypeInfo(SimpleNamespace):
    """A class to represent descriptive information for a idb packet type.

    Attributes
    ----------
    PID_SPID : `int`
        SCOS-2000 Telemetry Packet Number. Unsigned integer number in the range (1....2^32-1)
        (note that zero is not allowed).

    PID_DESCR : `str`
        Textual description of the SCOS-2000 telemetry packet (max 64 characters).

    PID_TPSD : `int`:
        SCOS-2000 Telemetry Packet Structure Definition. This field is only used by the Variable
        Packets Display application. It has to be set to `-1` for packets which are not defined in
        the VPD table and thus are not required to be processed by the Variable PacketsDisplay. If
        not set to –1, unsigned integer number in the range (1....2^31-1) (note that zero is not
        allowed).

    """

    def __init__(self, *, PID_SPID, PID_DESCR, PID_TPSD):
        super(IDBPacketTypeInfo, self).__init__(PID_SPID=PID_SPID, PID_DESCR=PID_DESCR,
                                                PID_TPSD=PID_TPSD)

    def is_variable(self):
        """Is the telemetry packet of variable length.

        Returns
        -------
        `bool`
            True if the TM packet has a variable size
        """
        return self.PID_TPSD != -1


class IDBPolynomialCalibration:
    """A class to represent a 4th order polynomial calibration defined in the IDB."""
    def __init__(self, rows):
        """Construct all the necessary attributes for the IDBPolynomialCalibration object.

        Parameters
        ----------
        rows : `list`
            the polynomial parameters from the IDB
        """
        try:
            self.orig = rows
            self.A = [float(row) for row in rows[0]]
            self.valid = True
        except (ValueError, IndexError):
            self.valid = False

    def __repr__(self):
        return f'{self.__class__.__name__}({self.orig})'

    def __call__(self, x):
        """Apply the polynomial function to the raw value.

        Parameters
        ----------
        x : `number`
            the raw value

        Returns
        -------
        `float`
            polynomial function value
        """
        x = np.array(x)
        res = (self.A[0] * x ** 0
               + self.A[1] * x ** 1
               + self.A[2] * x ** 2
               + self.A[3] * x ** 3
               + self.A[4] * x ** 4)

        return res.tolist() if self.valid else None


class IDBCalibrationCurve:
    """A class to represent a calibration curve for a LUT based interpolation defined in the IDB."""
    def __init__(self, rows, param):
        """Construct all the necessary attributes for the IDBCalibrationCurve object.

        Parameters
        ----------
        rows : `list`
            [x, y] all support points from the IDB
        param : `IDBCalibrationParameter`
        """
        try:
            self.x = [float(row[0]) for row in rows]
            self.y = [float(row[1]) for row in rows]
            self.valid = True
        except ValueError:
            self.valid = False

        self.param = param
        self.orig = rows

        if len(self) <= 1:
            logger.error(f'Invalid curve calibration parameter {param.PCF_NAME} / \
                        {param.PCF_CURTX}: at least two data points needed')
            self.valid = False

    def __repr__(self):
        return f'{self.__class__.__name__}({self.orig})'

    def __len__(self):
        return len(self.x)

    def __call__(self, raw):
        """Apply the interpolation function with the raw value based on the LUT provided by the IDB.

        Parameters
        ----------
        raw : `number`
            The raw value to apply to

        Returns
        -------
        `float`
            interpolated value
        """
        if not self.valid:
            return None
        if len(self) == 2:
            return ((self.y[1] - self.y[0]) /
                    (self.x[1] - self.x[0]) *
                    (raw - self.x[0]) + self.y[0])

        try:
            tck = interpolate.splrep(self.x, self.y)
            val = interpolate.splev(raw, tck)
            return val
        except Exception as e:
            logger.error(f'Failed to curve calibrate {self.param.PCF_NAME} / \
                        {self.param.PCF_CURTX} due to {e}')


class IDBParameter(IDBPacketTypeInfo):
    """A base class to represent a parameter of a SCOS-2000 Telemetry Packet.

    Attributes
    ----------
    PID_SPID : `int`
        SCOS-2000 Telemetry Packet Number the parameter belongs to. Unsigned integer number in the
        range (1....2^32-1) (note that zero is not allowed).

    PID_DESCR : `str`
        Textual description of the SCOS-2000 telemetry packet the parameter belongs to max 64
        charactars.

    PID_TPSD : `int`
        SCOS-2000 Telemetry Packet Structure Definition. This field is only used by the
        Variable Packets Display application. It has to be set to ‘-1’ for packets which are
        not defined in the VPD table and thus are not required to be processed by the Variable
        PacketsDisplay. If not set to –1, unsigned integer number in the range (1....2^31-1)
        (note that zero is not allowed).

    PCF_NAME : `str`
        Name of the parameter. Alphanumeric string uniquely identifying the monitoring
        parameter  (max 8 characters).

    PCF_DESCR : `str`
        Parameter Description - free textual description of the parameter.

    PCF_WIDTH : `int`
        'Padded' width of this parameter expressed in number of bits. This field is only used when
        extracting parameter samples using the VPD definition to identify the bitposition where the
        next telemetry parameter starts

    PCF_PFC : `int`
        Parameter Format Code. Along with the Parameter Type Code (PCF_PTC) this field controls the
        length of the parameter. Integer value in a range compatible with the specified PCF_PTC

    PCF_PTC : `int`
        Parameter Type Code. This controls the encoding format of the parameter. Integer value in
        the range (1..13).

    PCF_CURTX : `int`
        Parameter calibration identification name. Depending  on  parameter  category,  this  field
         stores  the  numerical calibration or the textual calibration identification name.

    2K_TYPE : `str`
        TBD.

    bin_format : `str`
        Read instruction format of the specific parameter for processing the bit stream e.g.
        "int:8". See `bitstream.ConstBitStream.read` for more information.
    """
    def __init__(self, *, PID_SPID, PID_DESCR, PID_TPSD, PCF_NAME, PCF_DESCR, PCF_WIDTH,
                 PCF_PFC, PCF_PTC, PCF_CURTX, S2K_TYPE, bin_format=''):
        super(IDBPacketTypeInfo, self).__init__(PID_SPID=PID_SPID, PID_DESCR=PID_DESCR,
                                                PID_TPSD=PID_TPSD)
        self.PCF_NAME = PCF_NAME
        self.PCF_DESCR = PCF_DESCR
        self.PCF_WIDTH = PCF_WIDTH
        self.PCF_PFC = PCF_PFC
        self.PCF_PTC = PCF_PTC
        self.PCF_CURTX = PCF_CURTX
        self.S2K_TYPE = S2K_TYPE
        self.bin_format = bin_format

    def get_product_attribut_name(self):
        return self.PCF_DESCR.lower().replace(' ', '_').replace('_-_', '-')


class IDBStaticParameter(IDBParameter):
    """A class to represent a parameter of a static SCOS-2000 Telemetry Packet.

    Attributes
    ----------
    PLF_OFFBY : `int`
        Location of first occurrence of parameter value in octets, relative to the end of the
        SCOS-2000 TM header. Integer value starting from 0 (negative values are not allowed).

    PLF_OFFBI : `int`
        Bit number, within an octet, of the first bit of the first occurrence of the parameter
        value. Bit 0 corresponds to the most left bit withinthe byte. Integer value in the range
        (0..7).

    """
    def __init__(self, *, PLF_OFFBY, PLF_OFFBI, **kwargs):
        super(IDBStaticParameter, self).__init__(**kwargs)
        self.PLF_OFFBY = PLF_OFFBY
        self.PLF_OFFBI = PLF_OFFBI

    @staticmethod
    def is_variable():
        """Is the parameter for a variable telemetry packet.

        Returns
        -------
        `bool`
            Always False for static parameters
        """
        return False


class IDBVariableParameter(IDBParameter):
    """A class to represent a parameter of a variable SCOS-2000 Telemetry Packet.

    Attributes
    ----------
    VPD_POS : `int`
        Ordinal position of this parameter inside the packet definition in ascending order.

    VPD_OFFSET : `int`
        Number of bits between the start position of this parameter and the end bit of
        the previous parameter in the packet. A positive offset enables the introduction of
        a ‘gap’ between the previous parameter and this one. A negative offset enables the
        ‘overlap’ of the bits contributing to this parameter with the ones contributing to
        the previous parameter(s). Integer value in the range (-32768..32767).

    VPD_GRPSIZE : `int`
        This value should only be set for parameters which identify a repeat counter N

    """

    def __init__(self, *, VPD_POS, VPD_OFFSET, VPD_GRPSIZE, **kwargs):
        super(IDBVariableParameter, self).__init__(**kwargs)
        self.VPD_POS = VPD_POS
        self.VPD_OFFSET = VPD_OFFSET
        self.VPD_GRPSIZE = VPD_GRPSIZE

    @staticmethod
    def is_variable():
        """Is the parameter for a variable telemetry packet.

        Returns
        -------
        `bool`
            Always True for this class
        """
        return True


class IDBCalibrationParameter(IDBParameter):
    """A class to represent a parameter for calibration.

    PCF_NAME': 'NIXD0167', 'PCF_CURTX': 'CAAT0033TM', 'PCF_CATEG': 'S', 'PCF_UNIT': None

    Attributes
    ----------
    PCF_CATEG : `str`
        Calibration category of the parameter one of N|S|T|R|D|P|H|S|C. STIX only uses (N)umeric and
         (S)tring at the moment.

    PCF_UNIT : `str`
        Engineering unit mnemonic of the parameter values e.g. ‘VOLT’ (max length 4).
    """

    def __init__(self, *, PCF_CATEG, PCF_UNIT, **kwargs):
        super(IDBCalibrationParameter, self).__init__(**kwargs)
        self.PCF_CATEG = PCF_CATEG
        self.PCF_UNIT = PCF_UNIT


class IDBPacketTree:
    """Class representing a dynamic telemetry packet of variable length in a tree structure
    with nested repeaters."""

    def __init__(self, *, children=None, counter=1, name='top', parameter=None):
        """Construct all the necessary attributes for the IDBPacketTree object.

        Parameters
        ----------
        children : `list`, optional
            list of IDBPacketTree, by default None: will be transformed to []
        counter : `int`, optional
            how often this parameter is repeated, by default 1
        name : `str`, optional
            unique name of the parameter, by default 'top'
        parameter : IDBParameter, optional
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
            List of `~stixcore/idb/idb/IDBPacketTree`
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
        `~stixcore/idb/idb/IDBParameter`
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
        self.packet_info = dict()
        self.parameter_units = dict()
        self.calibration_polynomial = dict()
        self.calibration = dict()
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

    @property
    def version(self):
        """Get the Version of the IDB.

        Returns
        -------
        `str`
            the Version label like '2.3.4' or None
        """
        return self._version

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
            # connect to the DB in read only mode
            uri = self.filename.as_uri() + "?mode=ro"

            if sys.version_info < (3, 7):
                self.conn = sqlite3.connect(uri, check_same_thread=False, uri=True)
            else:
                source = sqlite3.connect(uri, check_same_thread=False, uri=True)
                self.conn = sqlite3.connect(':memory:', check_same_thread=False)
                source.backup(self.conn)
                source.close()

            logger.info('IDB loaded from {}'.format(self.filename))
            self.cur = self.conn.cursor()
            self._version = self.get_idb_version()
        except sqlite3.Error:
            logger.error('Failed load IDB from {}'.format(self.filename))
            self.close()
            raise

    def __repr__(self):
        return f'{__class__.__name__}({self.version}, {self.filename})'

    def __getstate__(self):
        """Return state values to be pickled."""
        return self.filename

    def __setstate__(self, state):
        """Restore state from the unpickled state values."""
        self.filename = state

        self.parameter_structures = dict()
        self.parameter_units = dict()
        self.packet_info = dict()
        self.calibration_polynomial = dict()
        self.calibration = dict()
        self.calibration_curves = dict()
        self.textual_parameter_lut = dict()
        self.soc_descriptions = dict()
        self.parameter_descriptions = dict()
        self.s2k_table_contents = dict()

        if self.filename:
            self._connect_database()

    def close(self):
        """Close the IDB connection."""
        if self.conn:
            self.conn.close()
            self.cur = None
        else:
            logger.warning("IDB connection already closed")

    @classmethod
    def generate_calibration_name(cls, prefix, id, suffix="TM"):
        zeros = 10-len(prefix)-len(suffix)-len(str(id))
        name = prefix + ("0" * zeros) + str(id) + suffix
        return name, id + 1

    def _execute(self, sql, arguments=None, result_type='list'):
        """Execute sql and return results in a list or a dictionary."""
        if not self.cur:
            raise Exception('IDB is not initialized!')
        else:
            try:
                lock.acquire()
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
                return rows
            finally:
                lock.release()

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

    def get_packet_pi1_val_position(self, service_type, service_subtype):
        """Get offset and width for optional PI1_VAL for the packet defined by service type and
        subtype.

        Parameters
        ----------
        service_type : `int`
        service_subtype : `int`

        returns
        -------
        `IDBPi1ValPosition` or None
        """
        sql = ('select PIC_PI1_OFF, PIC_PI1_WID from PIC '
               'where PIC_TYPE = ? and PIC_STYPE = ? and PIC_PI1_OFF >= 0 limit 1')
        args = (service_type, service_subtype)
        res = self._execute(sql, args, result_type='dict')
        if res:
            return IDBPi1ValPosition(**res[0])

        return None

    def get_parameter_description(self, name):
        """Get scos long description.

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

    def get_packet_type_info(self, packet_type, packet_subtype, pi1_val=None):
        """Identify packet type using service, service subtype and information in IDB table PID.

        Parameters
        ----------
        packet_type : `int`
        packet_subtype : `int`
        pi1_val : `int`

        returns
        -------
        `IDBPacketTypeInfo` or `None` if not found
        """
        if (packet_type, packet_subtype, pi1_val) in self.packet_info:
            return self.packet_info[(packet_type, packet_subtype, pi1_val)]

        if pi1_val is None:
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
            resObj = IDBPacketTypeInfo(**rows[0])
            self.packet_info[(packet_type, packet_subtype, pi1_val)] = resObj
            return resObj

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

    def get_calibration_curve(self, param):
        """calibration curve defined in CAP database

        Parameters
        ----------
        param : `IDBCalibrationParameter`

        returns
        -------
        `IDBCalibrationCurve`
            calibration curve
        """
        if param.PCF_CURTX in self.calibration_curves:
            return self.calibration_curves[param.PCF_CURTX]
        else:
            sql = '''select cap_xvals, cap_yvals
                     from cap
                     where cap_numbr = ?
                     order by cast(CAP_XVALS as double) asc'''
            args = (param.PCF_CURTX, )
            curve = IDBCalibrationCurve(self._execute(sql, args), param)
            self.calibration_curves[param.PCF_CURTX] = curve
            return curve

    def textual_interpret(self, pcf_curtx, raw_value):
        """gets a name for a TXP_NUMBR from TXP for given raw_value

        Parameters
        ----------
        pcf_curtx : `str`
            TXP_NUMBR like 'CAAT0005TM'
        raw_value : `int`
            value in range of TXP_FROM  to TXP_TO

        returns
        -------
        `list` of `str`
            the names
        """
        if (pcf_curtx, raw_value) in self.textual_parameter_lut:
            return self.textual_parameter_lut[(pcf_curtx, raw_value)]

        sql = '''select TXP_ALTXT from TXP
                 where TXP_NUMBR = ?
                    and ? >= TXP_FROM
                    and TXP_TO >= ? limit 1'''
        args = (pcf_curtx, raw_value, raw_value)
        rows = self._execute(sql, args)
        val = rows[0][0] if rows else None

        if val is None:
            logger.error(f'Missing textual calibration info for: {pcf_curtx} value={raw_value}')
            # TODO discuss this fallback
            val = raw_value

        if val == "True":
            val = True
        elif val == "False":
            val = False

        self.textual_parameter_lut[(pcf_curtx, raw_value)] = val
        # lookup table
        return val

    def get_calibration_polynomial(self, mcf_ident):
        """gets calibration polynomial information for a given MCF_IDENT

        Parameters
        ----------
        mcf_ident : `str`
            TXP_NUMBR like 'CIX00036TM'

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
            poly = IDBPolynomialCalibration(self._execute(sql, args))
            self.calibration_polynomial[mcf_ident] = poly
            return poly

    def get_idb_version(self):
        """get the version string of the IDB

        returns
        -------
        `str`
            version label like "1.1.3"
        """
        try:
            sql = 'select version from IDB limit 1'
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
            see `~stixcore/idb/idb/IDBParameter.S2K_TYPE`
        nbytes : `int`
            see `~stixcore/idb/idb/IDBParameter.PCF_WIDTH`

        Returns
        -------
        `str`
            The format containing the data type and number of bits to read like "int:8".
        """
        if param_type == 'U':
            return f"uint:{nbytes}"
        elif param_type == 'I':
            return f"int:{nbytes}"
        elif param_type == 'T':
            return f"uint:{nbytes}"
        elif param_type == 'O':
            return f"uint:{nbytes}"
        elif param_type == 'CONTEXT' and nbytes <= 4:
            raise NotImplementedError("Format Error: to implement: 'CONTEXT'")

        raise NotImplementedError(f"Format Error: to implement: '{param_type}:{nbytes}")

    def get_static_structure(self, service_type, service_subtype, sp1_val):
        """Create a static parse struct for the specified TM packet.

        Parameters
        ----------
        service_type : `int`
            The TM packet service type.
        service_subtype : `int`
            The TM packet service subtype.

        Returns
        -------
        `~stixcore/idb/idb/IDBPacketTree`
            In this case the generic IDBPacketTree is flat, but can be used fore
            dynamic parseing anyway.
        """
        if (service_type, service_subtype, sp1_val) in self.parameter_structures:
            return self.parameter_structures[(service_type, service_subtype, sp1_val)]

        sql = (f'''SELECT
                    PID_SPID, PID_DESCR, PID_TPSD,
                    PCF.PCF_DESCR, PLF.PLF_OFFBY, PLF.PLF_OFFBI, PCF.PCF_NAME,
                    PCF.PCF_WIDTH, PCF.PCF_PFC,PCF.PCF_PTC, PCF.PCF_CURTX,
                    S2K_TYPE
                FROM PID , PLF, PCF, tblConfigS2KParameterTypes as PTYPE
                WHERE
                    PLF.PLF_NAME = PCF.PCF_NAME
                    AND PID_TYPE = ?
                    AND PID_STYPE = ?
                    {"AND PID_PI1_VAL = ? " if sp1_val is not None else " "}
                    AND PLF.PLF_SPID = PID.PID_SPID
                    AND PTYPE.PTC = PCF.PCF_PTC
                    AND PCF.PCF_PFC >= PTYPE.PFC_LB
                    AND PTYPE.PFC_UB >= PCF.PCF_PFC
                ORDER BY
                    PLF.PLF_OFFBY asc ''')
        args = (service_type, service_subtype)
        if sp1_val is not None:
            args = args + (sp1_val,)
        parameters = self._execute(sql, args, 'dict')

        parent = IDBPacketTree()
        for par in parameters:
            parObj = IDBStaticParameter(**par)
            node = self._create_parse_node(parObj.PCF_NAME, parObj, 0, [])
            parent.children.append(node)

        self.parameter_structures[(service_type, service_subtype, sp1_val)] = parent
        return parent

    def _create_parse_node(self, name, parameter=None, counter=0, children=None):

        if children is None:
            children = []

        parameter.bin_format = self._get_stream_type_format(parameter.S2K_TYPE, parameter.PCF_WIDTH)
        node = IDBPacketTree(name=name, counter=counter, parameter=parameter, children=children)
        return node

    def get_params_for_calibration(self, service_type, service_subtype, sp1_val=None,
                                   pcf_name=None, pcf_curtx=None):

        key = (service_type, service_type, service_subtype, sp1_val, pcf_name, pcf_curtx)
        if key in self.calibration:
            return self.calibration[key]
        else:
            sql = (f'''SELECT
                            PID_SPID, PID_DESCR, PID_TPSD, PCF.PCF_NAME, PCF.PCF_DESCR,
                            PCF.PCF_WIDTH, PCF.PCF_PFC, PCF.PCF_PTC, PCF.PCF_CURTX,
                            PCF.PCF_CATEG, PCF.PCF_UNIT, S2K_TYPE
                        FROM
                            PID, tblConfigS2KParameterTypes as PTYPE
                        LEFT JOIN PLF ON PLF.PLF_SPID = PID.PID_SPID
                        LEFT JOIN VPD ON VPD.VPD_TPSD = PID.PID_SPID
                        LEFT JOIN PCF ON PLF.PLF_NAME = PCF.PCF_NAME or VPD.VPD_NAME = PCF.PCF_NAME
                        WHERE
                            PCF.PCF_CURTX not NULL
                            AND PID_TYPE = ?
                            AND PID_STYPE = ?
                            AND PTYPE.PTC = PCF.PCF_PTC
                            AND PTYPE.PFC_UB >= PCF.PCF_PFC
                            AND PCF.PCF_PFC >= PTYPE.PFC_LB
                            {"AND PID_PI1_VAL = ? " if sp1_val is not None else " "}
                            {"AND PCF.PCF_NAME = ? " if pcf_name is not None else " "}
                            {"AND PCF.PCF_CURTX = ? " if pcf_curtx is not None else " "}
                        ''')
            args = (service_type, service_subtype)
            if sp1_val is not None:
                args = args + (sp1_val,)
            if pcf_name is not None:
                args = args + (pcf_name,)
            if pcf_curtx is not None:
                args = args + (pcf_curtx,)

            params = self._execute(sql, args, 'dict')
            self.calibration[key] = [IDBCalibrationParameter(**p) for p in params]
            return self.calibration[key]

    def get_variable_structure(self, service_type, service_subtype, sp1_val=None):
        """Create a dynamic parse tree for the specified TM packet.

        Parameters
        ----------
        service_type : `int`
            The TM packet service type.
        service_subtype : `int`
            The TM packet service subtype.
        PI1_VAL : `int` optional
            The TM packet optional PI1_VAL default `None`

        Returns
        -------
        `~stixcore/idb/idb/IDBPacketTree`
            The IDBPacketTree implements nested repeaters.
        """
        if (service_type, service_subtype, sp1_val) in self.parameter_structures:
            return self.parameter_structures[(service_type, service_subtype, sp1_val)]

        sql = (f'''SELECT
                    PID_SPID, PID_DESCR, PID_TPSD,
                    PCF.PCF_NAME, VPD.VPD_POS,PCF.PCF_WIDTH,PCF.PCF_PFC, PCF.PCF_PTC,VPD.VPD_OFFSET,
                    VPD.VPD_GRPSIZE,PCF.PCF_DESCR ,PCF.PCF_CURTX,
                    S2K_TYPE
                FROM PID , VPD, PCF, tblConfigS2KParameterTypes as PTYPE
                WHERE
                    PID_TYPE = ?
                    AND PID_STYPE = ?
                    {"AND PID_PI1_VAL = ? " if sp1_val is not None else " "}
                    AND VPD.VPD_NAME = PCF.PCF_NAME
                    AND VPD.VPD_TPSD = PID.PID_SPID
                    AND PTYPE.PTC = PCF.PCF_PTC
                    AND PCF.PCF_PFC >= PTYPE.PFC_LB
                    AND PTYPE.PFC_UB >= PCF.PCF_PFC
                ORDER BY
                    VPD.VPD_POS asc ''')
        args = (service_type, service_subtype)
        if sp1_val is not None:
            args = args + (sp1_val,)
        param_pcf_structures = self._execute(sql, args, 'dict')

        repeater = [{'node': IDBPacketTree(), 'counter': 1024}]

        for par in param_pcf_structures:
            parObj = IDBVariableParameter(**par)
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

        self.parameter_structures[(service_type, service_subtype, sp1_val)] = repeater[0]['node']
        return repeater[0]['node']
