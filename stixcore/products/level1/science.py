from collections import defaultdict

import numpy as np

import astropy.units as u
from astropy.table import unique
from astropy.table.operations import vstack

from stixcore.processing.engineering import raw_to_engineering_product
from stixcore.products.common import _get_energies_from_mask
from stixcore.products.product import BaseProduct
from stixcore.time import SCETimeRange

__all__ = ['RawPixelDataL1', 'CompressedPixelData', 'SummedPixelData', 'Visibility', 'Spectrogram',
           'Aspect']

from stixcore.tmtc.packets import GenericPacket


class ScienceProduct(BaseProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        self.service_type = service_type
        self.service_subtype = service_subtype
        self.ssid = ssid
        self.type = 'sci'
        self.control = control
        self.data = data
        self.idb_versions = kwargs.get('idb_versions', None)
        self.scet_timerange = kwargs['scet_timerange']

    @property
    def utc_timerange(self):
        return self.scet_timerange.to_timerange()

    @classmethod
    def from_level0(cls, level0, idbm=GenericPacket.idb_manager):
        l1 = cls(service_type=level0.service_type,
                 service_subtype=level0.service_subtype,
                 ssid=level0.ssid,
                 control=level0.control,
                 data=level0.data,
                 idb_versions=level0.idb_versions,
                 scet_timerange=level0.scet_timerange)

        raw_to_engineering_product(l1, idbm)
        return l1

    def __add__(self, other):
        if not isinstance(other, type(self)):
            raise TypeError(f'Products must of same type not {type(self)} and {type(other)}')

        if np.all(self.control == other.control) and np.all(self.data == other.data):
            return self

        combined_control_index = other.control['index'] + self.control['index'].max() + 1
        control = vstack((self.control, other.control))
        cnames = control.colnames
        cnames.remove('index')
        control = unique(control, cnames)

        combined_data_index = other.data['control_index'] + self.control['index'].max() + 1
        data = vstack((self.data, other.data))

        data_ind = np.isin(combined_data_index, combined_control_index)
        data = data[data_ind]

        # TODO fix
        idb_versions = self.idb_versions
        scet_timerange = SCETimeRange(start=data['time'][0] - data['timedel'][0]/2,
                                      end=data['time'][-1] + data['timedel'][-1]/2)

        return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                          ssid=self.ssid, data=data, control=control, idb_versions=idb_versions,
                          scet_timerange=scet_timerange)

    def to_requests(self):
        for ci in unique(self.control, keys=['tc_packet_seq_control', 'request_id'])['index']:
            control = self.control[self.control['index'] == ci]
            data = self.data[self.data['control_index'] == ci]
            # for req_id in self.control['request_id']:
            #     ctrl_inds = np.where(self.control['request_id'] == req_id)
            #     control = self.control[ctrl_inds]
            #     data_index = control['index'][0]
            #     data_inds = np.where(self.data['control_index'] == data_index)
            #     data = self.data[data_inds]

            scet_timerange = SCETimeRange(start=data['time'][0] - data['timedel'][0]/2,
                                          end=data['time'][-1] + data['timedel'][-1]/2)

            yield type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                             ssid=self.ssid, control=control, data=data,
                             scet_timerange=scet_timerange)

    def get_energies(self):
        if 'energy_bin_edge_mask' in self.control.colnames:
            energies = _get_energies_from_mask()
        elif 'energy_bin_mask' in self.control.colnames:
            energies = _get_energies_from_mask()
        else:
            energies = _get_energies_from_mask()

        return energies


class RawPixelDataL1(ScienceProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-rpd'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 20)


class CompressedPixelData(ScienceProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-cpd'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 21)


class SummedPixelData(ScienceProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-scpd'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 22)


class Visibility(ScienceProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-vis'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 23)


class Spectrogram(ScienceProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-spec'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 24)


class Aspect(ScienceProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'aspect-burst'
        self.level = 'L1'

    def to_days(self):
        utc_timerange = self.scet_timerange.to_timerange()

        for day in utc_timerange.get_dates():
            ds = day
            de = day + 1 * u.day
            utc_times = self.data['time'].to_time()
            i = np.where((utc_times >= ds) & (utc_times < de))

            if len(i[0]) > 0:
                scet_timerange = SCETimeRange(start=self.data['time'][i[0]]
                                              - self.data['timedel'][i[0]] / 2,
                                              end=self.data['time'][i[-1]]
                                              + self.data['timedel'][i[-1]] / 2)

                data = self.data[i]
                control_indices = np.unique(data['control_index'])
                control = self.control[np.isin(self.control['index'], control_indices)]
                control_index_min = control_indices.min()

                data['control_index'] = data['control_index'] - control_index_min
                control['index'] = control['index'] - control_index_min
                yield type(self)(service_type=self.service_type,
                                 service_subtype=self.service_subtype, ssid=self.ssid,
                                 control=control, data=data, idb_versions=self.idb_versions,
                                 scet_timerange=scet_timerange)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 42)
