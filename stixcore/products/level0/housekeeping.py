"""
House Keeping data products
"""
from stixcore.datetime.datetime import SCETime
from stixcore.products.level0.quicklook import QLProduct
from stixcore.products.product import Control, Data

__all__ = ['MiniReport', 'MaxiReport']


class MiniReport(QLProduct):
    """
    Mini house keeping reported during start up of the flight software.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'mini'
        self.level = 'L0'
        self.type = 'hk'

    @classmethod
    def from_levelb(cls, levelb):
        packets = super().from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control()
        control['scet_coarse'] = packets.get('scet_coarse')
        control['scet_fine'] = packets.get('scet_fine')
        control['integration_time'] = 0
        control['index'] = range(len(control))

        # Create array of times as dt from date_obs
        times = [SCETime(ct, ft).as_float() for ct, ft in control['scet_coarse', 'scet_fine']]

        # Data
        data = Data()
        data['time'] = times
        data['sw_running'] = packets.get_value('NIXD0021', attr='value')
        data['instrument_number'] = packets.get_value('NIXD0022', attr='value')
        data['instrument_mode'] = packets.get_value('NIXD0023', attr='value')
        data['hk_dpu_pcb_t'] = packets.get_value('NIXD0025')
        data['hk_dpu_fpga_t'] = packets.get_value('NIXD0026')
        data['hk_dpu_3v3_c'] = packets.get_value('NIXD0027')
        data['hk_dpu_2v5_c'] = packets.get_value('NIXD0028')
        data['hk_dpu_1v5_c'] = packets.get_value('NIXD0029')
        data['hk_dpu_spw_c'] = packets.get_value('NIXD0030')
        data['hk_dpu_spw0_v'] = packets.get_value('NIXD0031')
        data['hk_dpu_spw1_v'] = packets.get_value('NIXD0032')
        data['sw_version'] = packets.get_value('NIXD0001')
        data['cpu_load'] = packets.get_value('NIXD0002')
        data['archive_memory_usage'] = packets.get_value('NIXD0003')
        data['autonomous_asw_boot_stat'] = packets.get_value('NIXD0166', attr='value')
        data['memory_load_ena_flag'] = packets.get_value('NIXD0167', attr='value')
        data['idpu_identifier'] = packets.get_value('NIXD0004', attr='value')
        data['active_spw_link'] = packets.get_value('NIXD0005', attr='value')
        data['overruns_for_tasks'] = packets.get_value('NIXD0168', attr='value')
        data['watchdog_state'] = packets.get_value('NIXD0169', attr='value')
        data['received_spw_packets'] = packets.get_value('NIXD0079')
        data['rejected_spw_packets'] = packets.get_value('NIXD0078')
        data['hk_dpu_1v5_v'] = packets.get_value('NIXD0035')
        data['hk_ref_2v5_v'] = packets.get_value('NIXD0036')
        data['hk_dpu_2v9_v'] = packets.get_value('NIXD0037')
        data['hk_psu_temp_t'] = packets.get_value('NIXD0024')
        data['fdir_status'] = packets.get_value('NIX00085')
        data['fdir_status_mask_of_hk_temperature'] = packets.get_value('NIX00161')
        data['fdir_status_mask_of_hk_voltage'] = packets.get_value('NIX00162')
        data['hk_selftest_status_flag'] = packets.get_value('NIXD0163', attr='value')
        data['memory_status_flag'] = packets.get_value('NIXD0164', attr='value')
        data['fdir_status_mask_of_hk_current'] = packets.get_value('NIXD0165')
        data['number_executed_tc'] = packets.get_value('NIX00166', attr='value')
        data['number_sent_tm'] = packets.get_value('NIX00167', attr='value')
        data['number_failed_tm_gen'] = packets.get_value('NIX00168', attr='value')
        data['control_index'] = range(len(control))

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 3
                and service_subtype == 25 and ssid == 1)


class MaxiReport(QLProduct):
    """
    Maxi house keeping reported in all modes while the flight software is running.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'maxi'
        self.level = 'L0'
        self.type = 'hk'

    @classmethod
    def from_levelb(cls, levelb):
        packets = super().from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control()
        control['scet_coarse'] = packets.get('scet_coarse')
        control['scet_fine'] = packets.get('scet_fine')
        control['integration_time'] = 0
        control['index'] = range(len(control))

        # Create array of times as dt from date_obs
        times = [SCETime(ct, ft).as_float() for ct, ft in control['scet_coarse', 'scet_fine']]

        # Data
        data = Data()
        data['time'] = times
        data['sw_running'] = packets.get_value('NIXD0021', attr='value')
        data['instrument_number'] = packets.get_value('NIXD0022', attr='value')
        data['instrument_mode'] = packets.get_value('NIXD0023', attr='value')
        data['hk_dpu_pcb_t'] = packets.get_value('NIXD0025')
        data['hk_dpu_fpga_t'] = packets.get_value('NIXD0026')
        data['hk_dpu_3v3_c'] = packets.get_value('NIXD0027')
        data['hk_dpu_2v5_c'] = packets.get_value('NIXD0028')
        data['hk_dpu_1v5_c'] = packets.get_value('NIXD0029')
        data['hk_dpu_spw_c'] = packets.get_value('NIXD0030')
        data['hk_dpu_spw0_v'] = packets.get_value('NIXD0031')
        data['hk_dpu_spw1_v'] = packets.get_value('NIXD0032')
        data['hk_asp_ref_2v5a_v'] = packets.get_value('NIXD0038')
        data['hk_asp_ref_2v5b_v'] = packets.get_value('NIXD0039')
        data['hk_asp_tim01_t'] = packets.get_value('NIXD0040')
        data['hk_asp_tim02_t'] = packets.get_value('NIXD0041')
        data['hk_asp_tim03_t'] = packets.get_value('NIXD0042')
        data['hk_asp_tim04_t'] = packets.get_value('NIXD0043')
        data['hk_asp_tim05_t'] = packets.get_value('NIXD0044')
        data['hk_asp_tim06_t'] = packets.get_value('NIXD0045')
        data['hk_asp_tim07_t'] = packets.get_value('NIXD0046')
        data['hk_asp_tim08_t'] = packets.get_value('NIXD0047')
        data['hk_asp_vsensa_v'] = packets.get_value('NIXD0048')
        data['hk_asp_vsensb_v'] = packets.get_value('NIXD0049')
        data['hk_att_v'] = packets.get_value('NIXD0050')
        data['hk_att_t'] = packets.get_value('NIXD0051')
        data['hk_hv_01_16_v'] = packets.get_value('NIXD0052')
        data['hk_hv_17_32_v'] = packets.get_value('NIXD0053')
        data['det_q1_t'] = packets.get_value('NIXD0054')
        data['det_q2_t'] = packets.get_value('NIXD0055')
        data['det_q3_t'] = packets.get_value('NIXD0056')
        data['det_q4_t'] = packets.get_value('NIXD0057')
        data['hk_dpu_1v5_v'] = packets.get_value('NIXD0035')
        data['hk_ref_2v5_v'] = packets.get_value('NIXD0036')
        data['hk_dpu_2v9_v'] = packets.get_value('NIXD0037')
        data['hk_psu_temp_t'] = packets.get_value('NIXD0024')
        data['sw_version'] = packets.get_value('NIXD0001')
        data['cpu_load'] = packets.get_value('NIXD0002')
        data['archive_memory_usage'] = packets.get_value('NIXD0003')
        data['autonomous_asw_boot_stat'] = packets.get_value('NIXD0166', attr='value')
        data['memory_load_ena_flag'] = packets.get_value('NIXD0167', attr='value')
        data['idpu_identifier'] = packets.get_value('NIXD0004', attr='value')
        data['active_spw_link'] = packets.get_value('NIXD0005', attr='value')
        data['overruns_for_tasks'] = packets.get_value('NIXD0168', attr='value')
        data['watchdog_state'] = packets.get_value('NIXD0169', attr='value')
        data['received_spw_packetss'] = packets.get_value('NIXD0079')
        data['rejected_spw_packetss'] = packets.get_value('NIXD0078')
        data['endis_detector_status'] = packets.get_value('NIXD0070')
        data['spw1_power_status'] = packets.get_value('NIXD0080', attr='value')
        data['spw0_power_status'] = packets.get_value('NIXD0081', attr='value')
        data['q4_power_status'] = packets.get_value('NIXD0082', attr='value')
        data['q3_power_status'] = packets.get_value('NIXD0083', attr='value')
        data['q2_power_status'] = packets.get_value('NIXD0084', attr='value')
        data['q1_power_status'] = packets.get_value('NIXD0085', attr='value')
        data['aspect_b_power_status'] = packets.get_value('NIXD0086', attr='value')
        data['aspect_a_power_status'] = packets.get_value('NIXD0087', attr='value')
        data['att_m2_moving'] = packets.get_value('NIXD0088', attr='value')
        data['att_m1_moving'] = packets.get_value('NIXD0089', attr='value')
        data['hv17_32_enabled_status'] = packets.get_value('NIXD0090', attr='value')
        data['hv01_16_enabled_status'] = packets.get_value('NIXD0091', attr='value')
        data['lv_enabled_status'] = packets.get_value('NIXD0092', attr='value')
        data['hv1_depolar_in_progress'] = packets.get_value('NIXD0066', attr='value')
        data['hv2_depolar_in_progress'] = packets.get_value('NIXD0067', attr='value')
        data['att_ab_flag_open'] = packets.get_value('NIXD0068', attr='value')
        data['att_bc_flag_closed'] = packets.get_value('NIXD0069', attr='value')
        data['med_value_trg_acc'] = packets.get_value('NIX00072')
        data['max_value_of_trig_acc'] = packets.get_value('NIX00073')
        data['hv_regulators_mask'] = packets.get_value('NIXD0074', attr='value')
        data['tc_20_128_seq_cnt'] = packets.get_value('NIXD0077')
        data['attenuator_motions'] = packets.get_value('NIX00076')
        data['hk_asp_photoa0_v'] = packets.get_value('NIX00078')
        data['hk_asp_photoa1_v'] = packets.get_value('NIX00079')
        data['hk_asp_photob0_v'] = packets.get_value('NIX00080')
        data['hk_asp_photob1_v'] = packets.get_value('NIX00081')
        data['attenuator_currents'] = packets.get_value('NIX00094')
        data['hk_att_c'] = packets.get_value('NIXD0075')
        data['hk_det_c'] = packets.get_value('NIXD0058')
        data['fdir_function_status'] = packets.get_value('NIX00085')
        data['control_index'] = range(len(control))

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 3
                and service_subtype == 25 and ssid == 2)
