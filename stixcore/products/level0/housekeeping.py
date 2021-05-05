"""
House Keeping data products
"""
from collections import defaultdict

from stixcore.datetime.datetime import SCETime, SCETimeRange
from stixcore.products.level0.quicklook import QLProduct
from stixcore.products.product import Control, Data

__all__ = ['MiniReport', 'MaxiReport']


class MiniReport(QLProduct):
    """
    Mini house keeping reported during start up of the flight software.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'mini'
        self.level = 'L0'
        self.type = 'hk'

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = super().from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control()
        control['scet_coarse'] = packets.get('scet_coarse')
        control['scet_fine'] = packets.get('scet_fine')
        control['integration_time'] = 0
        control['index'] = range(len(control))

        # Create array of times as dt from date_obs
        times = SCETime(control['scet_coarse'], control['scet_fine'])
        scet_timerange = SCETimeRange(start=times[0], end=times[-1])

        # Data
        data = Data()
        data['time'] = times
        data.add_basic(name='sw_running', nix='NIXD0021', attr='value', packets=packets)
        data.add_basic(name='instrument_number', nix='NIXD0022', attr='value', packets=packets)
        data.add_basic(name='instrument_mode', nix='NIXD0023', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_pcb_t', nix='NIXD0025', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_fpga_t', nix='NIXD0026', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_3v3_c', nix='NIXD0027', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_2v5_c', nix='NIXD0028', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_1v5_c', nix='NIXD0029', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_spw_c', nix='NIXD0030', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_spw0_v', nix='NIXD0031', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_spw1_v', nix='NIXD0032', attr='value', packets=packets)
        data.add_basic(name='sw_version', nix='NIXD0001', packets=packets)
        data.add_basic(name='cpu_load', nix='NIXD0002', attr='value', packets=packets)
        data.add_basic(name='archive_memory_usage', nix='NIXD0003', attr='value', packets=packets)
        data.add_basic(name='autonomous_asw_boot_stat', nix='NIXD0166', attr='value',
                       packets=packets)
        data.add_basic(name='memory_load_ena_flag', nix='NIXD0167', attr='value', packets=packets)
        data.add_basic(name='idpu_identifier', nix='NIXD0004', attr='value', packets=packets)
        data.add_basic(name='active_spw_link', nix='NIXD0005', attr='value', packets=packets)
        data.add_basic(name='overruns_for_tasks', nix='NIXD0168', attr='value', packets=packets)
        data.add_basic(name='watchdog_state', nix='NIXD0169', attr='value', packets=packets)
        data.add_basic(name='received_spw_packets', nix='NIXD0079', packets=packets)
        data.add_basic(name='rejected_spw_packets', nix='NIXD0078', packets=packets)
        data.add_basic(name='hk_dpu_1v5_v', nix='NIXD0035', attr='value', packets=packets)
        data.add_basic(name='hk_ref_2v5_v', nix='NIXD0036', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_2v9_v', nix='NIXD0037', attr='value', packets=packets)
        data.add_basic(name='hk_psu_temp_t', nix='NIXD0024', attr='value', packets=packets)
        data.add_basic(name='fdir_status', nix='NIX00085', packets=packets)
        data.add_basic(name='fdir_status_mask_of_hk_temperature', nix='NIX00161', packets=packets)
        data.add_basic(name='fdir_status_mask_of_hk_voltage', nix='NIX00162', packets=packets)
        data.add_basic(name='hk_selftest_status_flag', nix='NIXD0163', attr='value',
                       packets=packets)
        data.add_basic(name='memory_status_flag', nix='NIXD0164', attr='value', packets=packets)
        data.add_basic(name='fdir_status_mask_of_hk_current', nix='NIXD0165', packets=packets)
        data.add_basic(name='number_executed_tc', nix='NIX00166', attr='value', packets=packets)
        data.add_basic(name='number_sent_tm', nix='NIX00167', attr='value', packets=packets)
        data.add_basic(name='number_failed_tm_gen', nix='NIX00168', attr='value', packets=packets)
        data['control_index'] = range(len(control))

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 3
                and service_subtype == 25 and ssid == 1)


class MaxiReport(QLProduct):
    """
    Maxi house keeping reported in all modes while the flight software is running.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'maxi'
        self.level = 'L0'
        self.type = 'hk'

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = super().from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control()
        control['scet_coarse'] = packets.get('scet_coarse')
        control['scet_fine'] = packets.get('scet_fine')
        control['integration_time'] = 0
        control['index'] = range(len(control))

        # Create array of times as dt from date_obs
        times = SCETime(control['scet_coarse'], control['scet_fine'])
        scet_timerange = SCETimeRange(start=times[0], end=times[-1])

        # Data
        data = Data()
        data['time'] = times
        data.add_basic(name='sw_running', nix='NIXD0021', packets=packets)
        data.add_basic(name='instrument_number', nix='NIXD0022', packets=packets)
        data.add_basic(name='instrument_mode', nix='NIXD0023', packets=packets)
        data.add_basic(name='hk_dpu_pcb_t', nix='NIXD0025', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_fpga_t', nix='NIXD0026', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_3v3_c', nix='NIXD0027', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_2v5_c', nix='NIXD0028', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_1v5_c', nix='NIXD0029', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_spw_c', nix='NIXD0030', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_spw0_v', nix='NIXD0031', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_spw1_v', nix='NIXD0032', attr='value', packets=packets)
        data.add_basic(name='hk_asp_ref_2v5a_v', nix='NIXD0038', attr='value', packets=packets)
        data.add_basic(name='hk_asp_ref_2v5b_v', nix='NIXD0039', attr='value', packets=packets)
        data.add_basic(name='hk_asp_tim01_t', nix='NIXD0040', attr='value', packets=packets)
        data.add_basic(name='hk_asp_tim02_t', nix='NIXD0041', attr='value', packets=packets)
        data.add_basic(name='hk_asp_tim03_t', nix='NIXD0042', attr='value', packets=packets)
        data.add_basic(name='hk_asp_tim04_t', nix='NIXD0043', attr='value', packets=packets)
        data.add_basic(name='hk_asp_tim05_t', nix='NIXD0044', attr='value', packets=packets)
        data.add_basic(name='hk_asp_tim06_t', nix='NIXD0045', attr='value', packets=packets)
        data.add_basic(name='hk_asp_tim07_t', nix='NIXD0046', attr='value', packets=packets)
        data.add_basic(name='hk_asp_tim08_t', nix='NIXD0047', attr='value', packets=packets)
        data.add_basic(name='hk_asp_vsensa_v', nix='NIXD0048', attr='value', packets=packets)
        data.add_basic(name='hk_asp_vsensb_v', nix='NIXD0049', attr='value', packets=packets)
        data.add_basic(name='hk_att_v', nix='NIXD0050', attr='value', packets=packets)
        data.add_basic(name='hk_att_t', nix='NIXD0051', attr='value', packets=packets)
        data.add_basic(name='hk_hv_01_16_v', nix='NIXD0052', attr='value', packets=packets)
        data.add_basic(name='hk_hv_17_32_v', nix='NIXD0053', attr='value', packets=packets)
        data.add_basic(name='det_q1_t', nix='NIXD0054', attr='value', packets=packets)
        data.add_basic(name='det_q2_t', nix='NIXD0055', attr='value', packets=packets)
        data.add_basic(name='det_q3_t', nix='NIXD0056', attr='value', packets=packets)
        data.add_basic(name='det_q4_t', nix='NIXD0057', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_1v5_v', nix='NIXD0035', attr='value', packets=packets)
        data.add_basic(name='hk_ref_2v5_v', nix='NIXD0036', attr='value', packets=packets)
        data.add_basic(name='hk_dpu_2v9_v', nix='NIXD0037', attr='value', packets=packets)
        data.add_basic(name='hk_psu_temp_t', nix='NIXD0024', attr='value', packets=packets)
        data.add_basic(name='sw_version', nix='NIXD0001', attr='value', packets=packets)
        data.add_basic(name='cpu_load', nix='NIXD0002', attr='value', packets=packets)
        data.add_basic(name='archive_memory_usage', attr='value', nix='NIXD0003', packets=packets)
        data.add_basic(name='autonomous_asw_boot_stat', nix='NIXD0166', attr='value',
                       packets=packets)
        data.add_basic(name='memory_load_ena_flag', nix='NIXD0167', attr='value', packets=packets)
        data.add_basic(name='idpu_identifier', nix='NIXD0004', attr='value', packets=packets)
        data.add_basic(name='active_spw_link', nix='NIXD0005', attr='value', packets=packets)
        data.add_basic(name='overruns_for_tasks', nix='NIXD0168', attr='value', packets=packets)
        data.add_basic(name='watchdog_state', nix='NIXD0169', attr='value', packets=packets)
        data.add_basic(name='received_spw_packetss', nix='NIXD0079', packets=packets)
        data.add_basic(name='rejected_spw_packetss', nix='NIXD0078', packets=packets)
        data.add_basic(name='endis_detector_status', nix='NIXD0070', packets=packets)
        data.add_basic(name='spw1_power_status', nix='NIXD0080', attr='value', packets=packets)
        data.add_basic(name='spw0_power_status', nix='NIXD0081', attr='value', packets=packets)
        data.add_basic(name='q4_power_status', nix='NIXD0082', attr='value', packets=packets)
        data.add_basic(name='q3_power_status', nix='NIXD0083', attr='value', packets=packets)
        data.add_basic(name='q2_power_status', nix='NIXD0084', attr='value', packets=packets)
        data.add_basic(name='q1_power_status', nix='NIXD0085', attr='value', packets=packets)
        data.add_basic(name='aspect_b_power_status', nix='NIXD0086', attr='value', packets=packets)
        data.add_basic(name='aspect_a_power_status', nix='NIXD0087', attr='value', packets=packets)
        data.add_basic(name='att_m2_moving', nix='NIXD0088', attr='value', packets=packets)
        data.add_basic(name='att_m1_moving', nix='NIXD0089', attr='value', packets=packets)
        data.add_basic(name='hv17_32_enabled_status', nix='NIXD0090', attr='value', packets=packets)
        data.add_basic(name='hv01_16_enabled_status', nix='NIXD0091', attr='value', packets=packets)
        data.add_basic(name='lv_enabled_status', nix='NIXD0092', attr='value', packets=packets)
        data.add_basic(name='hv1_depolar_in_progress', nix='NIXD0066', attr='value',
                       packets=packets)
        data.add_basic(name='hv2_depolar_in_progress', nix='NIXD0067', attr='value',
                       packets=packets)
        data.add_basic(name='att_ab_flag_open', nix='NIXD0068', attr='value', packets=packets)
        data.add_basic(name='att_bc_flag_closed', nix='NIXD0069', attr='value', packets=packets)
        data.add_basic(name='med_value_trg_acc', nix='NIX00072', packets=packets)
        data.add_basic(name='max_value_of_trig_acc', nix='NIX00073', packets=packets)
        data.add_basic(name='hv_regulators_mask', nix='NIXD0074', attr='value', packets=packets)
        data.add_basic(name='tc_20_128_seq_cnt', nix='NIXD0077', packets=packets)
        data.add_basic(name='attenuator_motions', nix='NIX00076', packets=packets)
        data.add_basic(name='hk_asp_photoa0_v', nix='NIX00078', attr='value', packets=packets)
        data.add_basic(name='hk_asp_photoa1_v', nix='NIX00079', attr='value', packets=packets)
        data.add_basic(name='hk_asp_photob0_v', nix='NIX00080', attr='value', packets=packets)
        data.add_basic(name='hk_asp_photob1_v', nix='NIX00081', attr='value', packets=packets)
        data.add_basic(name='attenuator_currents', nix='NIX00094', attr='value', packets=packets)
        data.add_basic(name='hk_att_c', nix='NIXD0075', attr='value', packets=packets)
        data.add_basic(name='hk_det_c', nix='NIXD0058', attr='value', packets=packets)
        data.add_basic(name='fdir_function_status', nix='NIX00085', packets=packets)
        data['control_index'] = range(len(control))

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 3
                and service_subtype == 25 and ssid == 2)
