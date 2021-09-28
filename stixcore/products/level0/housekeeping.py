"""
House Keeping data products
"""
import logging
from collections import defaultdict

import numpy as np

from astropy.table import unique, vstack

from stixcore.products.level0.quicklook import QLProduct
from stixcore.products.product import Control, Data
from stixcore.time import SCETime, SCETimeDelta, SCETimeRange
from stixcore.util.logging import get_logger

logger = get_logger(__name__, level=logging.DEBUG)

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
        data['timedel'] = SCETimeDelta(0, 0)
        for nix, param in packets.data[0].__dict__.items():

            if nix.startswith("NIXG") or nix == 'NIX00020':
                continue

            name = param.idb_info.PCF_DESCR.lower().replace(' ', '_')
            data.add_basic(name=name, nix=nix, attr='value', packets=packets)

        data['control_index'] = range(len(control))

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    def __add__(self, other):
        """
        Combine two products stacking data along columns and removing duplicated data using time as
        the primary key.

        Parameters
        ----------
        other : A subclass of stix_parser.products.quicklook.QLProduct

        Returns
        -------
        A subclass of stix_parser.products.quicklook.QLProduct
            The combined data product
        """
        if not isinstance(other, type(self)):
            raise TypeError(f'Products must of same type not {type(self)} and {type(other)}')

        # TODO reindex and update data control_index
        other_control = other.control[:]
        other_data = other.data[:]
        other_control['index'] = other.control['index'] + self.control['index'].max() + 1
        control = vstack((self.control, other_control))
        # control = unique(control, keys=['scet_coarse', 'scet_fine'])
        # control = control.group_by(['scet_coarse', 'scet_fine'])

        other_data['control_index'] = other.data['control_index'] + self.control['index'].max() + 1

        logger.debug('len self: %d, len other %d', len(self.data), len(other_data))

        data = vstack((self.data, other_data))

        logger.debug('len stacked %d', len(data))

        # Not sure where the rounding issue is arising need to investigate
        data['time_float'] = np.around(data['time'].as_float(), 2)

        data = unique(data, keys=['time_float'])

        logger.debug('len unique %d', len(data))

        data.remove_column('time_float')

        unique_control_inds = np.unique(data['control_index'])
        control = control[np.nonzero(control['index'][:, None] == unique_control_inds)[1]]

        for idb_key, date_range in other.idb_versions.items():
            self.idb_versions[idb_key].expand(date_range)

        scet_timerange = SCETimeRange(start=data['time'][0]-data['timedel'][0]/2,
                                      end=data['time'][-1]+data['timedel'][-1]/2)

        return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                          ssid=self.ssid, control=control, data=data,
                          idb_versions=self.idb_versions, scet_timerange=scet_timerange)

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
        data['timedel'] = SCETimeDelta(0, 0)

        for nix, param in packets.data[0].__dict__.items():

            if nix.startswith("NIXG") or nix == 'NIX00020':
                continue

            name = param.idb_info.PCF_DESCR.lower().replace(' ', '_')
            data.add_basic(name=name, nix=nix, attr='value', packets=packets)

        data['control_index'] = range(len(control))

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    def __add__(self, other):
        """
        Combine two products stacking data along columns and removing duplicated data using time as
        the primary key.

        Parameters
        ----------
        other : A subclass of stix_parser.products.quicklook.QLProduct

        Returns
        -------
        A subclass of stix_parser.products.quicklook.QLProduct
            The combined data product
        """
        if not isinstance(other, type(self)):
            raise TypeError(f'Products must of same type not {type(self)} and {type(other)}')

        # TODO reindex and update data control_index
        other_control = other.control[:]
        other_data = other.data[:]
        other_control['index'] = other.control['index'] + self.control['index'].max() + 1
        control = vstack((self.control, other_control))
        # control = unique(control, keys=['scet_coarse', 'scet_fine'])
        # control = control.group_by(['scet_coarse', 'scet_fine'])

        other_data['control_index'] = other.data['control_index'] + self.control['index'].max() + 1

        logger.debug('len self: %d, len other %d', len(self.data), len(other_data))

        data = vstack((self.data, other_data))

        logger.debug('len stacked %d', len(data))

        # Not sure where the rounding issue is arising need to investigate
        data['time_float'] = np.around(data['time'].as_float(), 2)

        data = unique(data, keys=['time_float'])

        logger.debug('len unique %d', len(data))

        data.remove_column('time_float')

        unique_control_inds = np.unique(data['control_index'])
        control = control[np.nonzero(control['index'][:, None] == unique_control_inds)[1]]

        for idb_key, date_range in other.idb_versions.items():
            self.idb_versions[idb_key].expand(date_range)

        scet_timerange = SCETimeRange(start=data['time'][0]-data['timedel'][0]/2,
                                      end=data['time'][-1]+data['timedel'][-1]/2)

        return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                          ssid=self.ssid, control=control, data=data,
                          idb_versions=self.idb_versions, scet_timerange=scet_timerange)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 3
                and service_subtype == 25 and ssid == 2)
