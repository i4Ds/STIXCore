import os
import sqlite3
import threading

thread_lock = threading.Lock()

__all__ = ['IDB']

class IDB(object):
    """
    Provides reading functionality to a IDB (definition of TM/TC packet structures)

    """
    def __init__(self, filename='', utc=None):
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

        print('loading idb from: ', self.filename)

        self.num_trials = 0
        if self.filename:
            self.connect_database(self.filename)

    def is_connected(self):
        if self.cur:
            return True
        return False

    def reload(self, filename):
        if filename == self.filename:
            #logger.info('IDB already loaded')
            return
        self.filename = filename
        self.close()
        self.parameter_structures = dict()
        self.calibration_polynomial = dict()
        self.calibration_curves = dict()
        self.textual_parameter_lut = dict()
        self.soc_descriptions = dict()
        self.parameter_descriptions = dict()
        self.s2k_table_contents = dict()
        if self.filename:
            self.connect_database(self.filename)

    def get_idb_filename(self):
        return os.path.abspath(self.filename)

    def connect_database(self, filename):
        self.filename = filename
        try:
            self.conn = sqlite3.connect(filename, check_same_thread=False)
            #logger.info('IDB loaded from {}'.format(filename))
            self.cur = self.conn.cursor()
        except sqlite3.Error:
            #logger.error('Failed load IDB from {}'.format(filename))
            1

    def close(self):
        if self.conn:
            self.conn.close()
            self.cur = None

    def execute(self, sql, arguments=None, result_type='list'):
        """
        execute sql and return results in a list or a dictionary
        """
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
        """ get SPID description """
        sql = 'select PID_DESCR,PID_TYPE,PID_STYPE from PID where PID_SPID=? limit 1'
        return self.execute(sql, (spid, ))

    def print_all_spid_desc(self):
        sql = 'select PID_DESCR,PID_SPID from PID'
        rows = self.execute(sql, None)
        for row in rows:
            print('"{}":"{}",'.format(row[1], row[0]))

    def get_scos_description(self, name):
        """ get scos long description """
        if name in self.soc_descriptions:
            return self.soc_descriptions[name]
        else:
            rows = self.execute(
                'select SW_DESCR from sw_para where scos_name=? ', (name, ))
            if rows:
                res = rows[0][0]
                self.soc_descriptions[name] = res
                return res
            return ''

    def get_telemetry_description(self, spid):
        """get telemetry data information """
        sql = ('select sw_para.SW_DESCR, tpcf.tpcf_name  '
               ' from sw_para join tpcf '
               'on tpcf.tpcf_name=sw_para.scos_name and tpcf.tpcf_spid= ?')
        return self.execute(sql, (spid, ))

    def get_packet_type_offset(self, packet_type, packet_subtype):
        sql = ('select PIC_PI1_OFF, PIC_PI1_WID from PIC '
               'where PIC_TYPE=? and PIC_STYPE=? limit 1')
        args = (packet_type, packet_subtype)
        rows = self.execute(sql, args)
        if rows:
            return rows[0]
        return 0, 0

    def get_parameter_description(self, name):
        """ get scos long description """
        if name in self.parameter_descriptions:
            return self.parameter_descriptions[name]
        else:
            rows = self.execute('select PCF_DESCR from PCF where PCF_NAME=? ',
                                (name, ))
            if not rows:
                rows = self.execute(
                    'select CPC_DESCR from CPC where CPC_PNAME=? ', (name, ))
            if rows:
                res = rows[0][0]
                self.parameter_descriptions[name] = res
                return res
            return ''

    def get_parameter_unit(self, name):
        if not self.parameter_units:
            results = self.execute(
                'select PCF_NAME, PCF_UNIT from PCF where PCF_UNIT!=""')
            self.parameter_units = {row[0]: row[1] for row in results}
        if name in self.parameter_units:
            return self.parameter_units[name]
        return ''

    def get_packet_type_info(self, packet_type, packet_subtype, pi1_val=-1):
        """
        Identify packet type using service, service subtype and information in IDB table PID
        """
        args = None
        if pi1_val == -1:
            sql = ('select PID_SPID, PID_DESCR, PID_TPSD from PID '
                   'where PID_TYPE=? and PID_STYPE=? limit 1')
            args = (packet_type, packet_subtype)
        else:
            sql = (
                'select PID_SPID, PID_DESCR, PID_TPSD from PID '
                'where PID_TYPE=? and PID_STYPE=? and PID_PI1_VAL=? limit 1')
            args = (packet_type, packet_subtype, pi1_val)
        rows = self.execute(sql, args, 'dict')
        if rows:
            return rows[0]
        else:
            print(
                "No information in IDB for service {}, service_subtype {}  and pi1_val: {} "
                .format(packet_type, packet_subtype, pi1_val))
            return None

    def get_s2k_parameter_types(self, ptc, pfc):
        """ get parameter type """
        if (ptc, pfc) in self.s2k_table_contents:
            return self.s2k_table_contents[(ptc, pfc)]
        else:
            sql = ('select S2K_TYPE from '
                   ' tblConfigS2KParameterTypes where PTC = ? '
                   ' and ? >= PFC_LB and  PFC_UB >= ? limit 1')
            args = (ptc, pfc, pfc)
            rows = self.execute(sql, args, 'list')
            s2k_type = rows[0][0]
            self.s2k_table_contents[(ptc, pfc)] = s2k_type
            return s2k_type

    def convert_NIXG_NIXD(self, name):
        sql = (
            'select PDI_GLOBAL, PDI_DETAIL, PDI_OFFSET from PDI where PDI_GLOBAL=? '
        )
        args = (name, )
        rows = self.execute(sql, args, 'dict')
        return rows

    def get_fixed_packet_structure(self, spid):
        """
        get parameter structures using SCO ICD (page 39)
        Args:
            spid: SPID
        Returns:
            is_fixed: whether it is a fixed length packet
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
        res = self.execute(sql, args, 'dict')
        self.parameter_structures[spid] = res
        return res

    def get_telecommand_info(self, header):
        """
            get TC description
        """
        service_type = header['service_type']
        service_subtype = header['service_subtype']
        sql = (
            'select  CCF_CNAME, CCF_DESCR, CCF_DESCR2, '
            ' CCF_NPARS from CCF where CCF_TYPE=? and CCF_STYPE =? order by CCF_CNAME asc'
        )
        res = self.execute(sql, (service_type, service_subtype), 'dict')
        index = 0
        if len(res) > 1 and 'subtype' in header:
            index = header['subtype'] - 1
        try:
            return res[index]
        except IndexError:
            return None

    def get_telecommand_structure(self, name):
        """
            Get the structure of a telecommand  by its name
            The structure will be used to decode the TC packet.
        """
        sql = ('select CDF_ELTYPE, CDF_DESCR, CDF_ELLEN, CDF_BIT, '
               'CDF_GRPSIZE, CDF_PNAME, CPC_DESCR,  CPC_PAFREF, CPC_PTC,'
               'CPC_PFC from CDF left join CPC on  CDF_PNAME=CPC_PNAME'
               '  where  CDF_CNAME=?  order by CDF_BIT asc')
        args = (name, )
        res = self.execute(sql, args, 'dict')
        return res

    def is_variable_length_telecommand(self, name):
        sql = 'select CDF_GRPSIZE  from CDF where CDF_GRPSIZE >0 and CDF_CNAME=?'
        args = (name, )
        rows = self.execute(sql, args, 'list')
        if rows:
            num_repeater = int(rows[0][0])
            if num_repeater > 0:
                return True
        return False

    def get_variable_packet_structure(self, spid):
        if spid in self.parameter_structures:
            return self.parameter_structures[spid]
        sql = (
            'select PCF.PCF_NAME,  VPD.VPD_POS,PCF.PCF_WIDTH,PCF.PCF_PFC, PCF.PCF_PTC,VPD.VPD_OFFSET,'
            ' VPD.VPD_GRPSIZE,PCF.PCF_DESCR ,PCF.PCF_CURTX'
            ' from VPD inner join PCF on  VPD.VPD_NAME=PCF.PCF_NAME and VPD.VPD_TPSD=? order by '
            ' VPD.VPD_POS asc')
        res = self.execute(sql, (spid, ), 'dict')
        self.parameter_structures[spid] = res
        return res

    def tcparam_interpret(self, ref, raw):
        """
         interpret telecommand parameter by using the table PAS
        """
        sql = 'select PAS_ALTXT from PAS where PAS_NUMBR=? and PAS_ALVAL=?'
        args = (ref, raw)
        rows = self.execute(sql, args)
        try:
            return rows[0][0]
        except (TypeError, IndexError):
            return ''
        return ''

    def get_calibration_curve(self, pcf_curtx):
        """ calibration curve defined in CAP database """
        if pcf_curtx in self.calibration_curves:
            return self.calibration_curves[pcf_curtx]
        else:
            sql = ('select cap_xvals, cap_yvals from cap '
                   ' where cap_numbr=? order by cast(CAP_XVALS as double) asc')
            args = (pcf_curtx, )
            rows = self.execute(sql, args)
            self.calibration_curves[pcf_curtx] = rows
            return rows

    def get_textual_mapping(self, parameter_name):
        sql = 'select  TXP_FROM, TXP_ALTXT from TXP join PCF on PCF_CURTX=TXP_NUMBR where PCF_NAME=? order by TXP_FROM asc'
        args = (parameter_name, )
        rows = self.execute(sql, args)
        if rows:
            return ([int(x[0]) for x in rows], [x[1] for x in rows])
        else:
            return None

    def textual_interpret(self, pcf_curtx, raw_value):
        if (pcf_curtx, raw_value) in self.textual_parameter_lut:
            # build a lookup table
            return self.textual_parameter_lut[(pcf_curtx, raw_value)]

        sql = ('select TXP_ALTXT from TXP where  TXP_NUMBR=? and ?>=TXP_FROM '
               ' and TXP_TO>=? limit 1')
        args = (pcf_curtx, raw_value, raw_value)
        rows = self.execute(sql, args)
        self.textual_parameter_lut[(pcf_curtx, raw_value)] = rows
        # lookup table
        return rows

    def get_calibration_polynomial(self, pcf_curtx):
        if pcf_curtx in self.calibration_polynomial:
            return self.calibration_polynomial[pcf_curtx]
        else:
            sql = ('select MCF_POL1, MCF_POL2, MCF_POL3, MCF_POL4, MCF_POL5 '
                   'from MCF where MCF_IDENT=? limit 1')
            args = (pcf_curtx, )
            rows = self.execute(sql, args)
            self.calibration_polynomial[pcf_curtx] = rows
            return rows

    def get_idb_version(self):
        try:
            sql = ('select version from IDB limit 1')
            rows = self.execute(sql, None, 'list')
            return rows[0][0]
        except (sqlite3.OperationalError, IndexError):
            #logger.warning('No IDB version information found in IDB')
            return '-1'
