from pathlib import Path
from datetime import datetime, timedelta

import numpy as np

from astropy.io import fits
from astropy.table import QTable

from stixcore.ephemeris.manager import Spice
from stixcore.io.FlareListProductsStorage import ProcessingHistoryStorage
from stixcore.processing.SingleStep import SingleProductProcessingStepMixin, TestForProcessingResult
from stixcore.processing.sswidl import SSWIDLProcessor
from stixcore.products.ANC.aspect import AspectIDLProcessing, Ephemeris
from stixcore.products.product import Product
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger
from stixcore.util.util import get_incomplete_file_name_and_path

__all__ = ['AspectANC']

logger = get_logger(__name__)


class AspectANC(SingleProductProcessingStepMixin):
    """Processing step from a flare list entry to L3.
    """
    INPUT_PATTERN = "solo_L1_stix-hk-maxi_*.fits"

    def __init__(self, source_dir, output_dir):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)

    def test_for_processing(self, candidate: Path,
                            phm: ProcessingHistoryStorage) -> TestForProcessingResult:

        try:
            c_header = fits.getheader(candidate)
            f_data_end = datetime.fromisoformat(c_header['DATE-END'])
            f_create_date = datetime.fromisoformat(c_header['DATE'])
            f_version = int(c_header['VERSION'])

            was_processed = phm.has_processed_fits_products('ephemeris', 'ANC', 'asp',
                                                            Ephemeris.get_cls_processing_version(),
                                                            str(candidate), f_create_date)

            # found already in the processing history
            if was_processed:
                return TestForProcessingResult.NotSuitable

            # test if the version of the input fits files is older as the target version
            # TODO check if this is not a to restrictive check
            if f_version < Ephemeris.get_cls_processing_version():
                return TestForProcessingResult.NotSuitable

            # safety margin of 1day until we start with processing of HK files into ANC-asp files
            # only use flown spice kernels not predicted once as pointing information
            # can be "very off"
            if (f_data_end > (Spice.instance.get_mk_date(meta_kernel_type="flown")
                              - timedelta(hours=24))):
                return TestForProcessingResult.NotSuitable

            return TestForProcessingResult.Suitable
        except Exception as e:
            logger.error(e)
        return TestForProcessingResult.NotSuitable

    def process_fits_files(self, files, *, soopmanager: SOOPManager,
                           spice_kernel_path: Path, processor, config):
        CONFIG = config
        max_idlbatch = CONFIG.getint("IDLBridge", "batchsize", fallback=20)
        SOOPManager.instance = soopmanager
        Spice.instance = Spice(spice_kernel_path)
        all_files = list()

        idlprocessor = SSWIDLProcessor(processor)

        for pf in files:
            l1hk = Product(pf)

            data = QTable()
            data['cha_diode0'] = l1hk.data['hk_asp_photoa0_v']
            data['cha_diode1'] = l1hk.data['hk_asp_photoa1_v']
            data['chb_diode0'] = l1hk.data['hk_asp_photob0_v']
            data['chb_diode1'] = l1hk.data['hk_asp_photob1_v']
            data['time'] = [d.strftime('%Y-%m-%dT%H:%M:%S.%f')
                            for d in l1hk.data['time'].to_datetime()]
            data['scet_time_f'] = l1hk.data['time'].fine
            data['scet_time_c'] = l1hk.data['time'].coarse

            # TODO set to seconds
            dur = (l1hk.data['time'][1:] - l1hk.data['time'][0:-1]).as_float().value
            data['duration'] = dur[0]
            data['duration'][0:-1] = dur
            data['duration'][:] = dur[-1]

            data['spice_disc_size'] = [Spice.instance.get_sun_disc_size(date=d)
                                       for d in l1hk.data['time']]

            data['y_srf'] = 0.0
            data['z_srf'] = 0.0
            data['calib'] = 0.0
            data['sas_ok'] = np.byte(0)
            data['error'] = ""
            data['control_index'] = l1hk.data['control_index']

            dataobj = dict()
            for coln in data.colnames:
                dataobj[coln] = data[coln].value.tolist()

            f = {'parentfits': str(get_incomplete_file_name_and_path(pf)),
                 'data': dataobj}

            idlprocessor[AspectIDLProcessing].params['hk_files'].append(f)
            idlprocessor.opentasks += 1

            if idlprocessor.opentasks >= max_idlbatch:
                all_files.extend(idlprocessor.process())
                idlprocessor = SSWIDLProcessor(processor)

        if idlprocessor.opentasks > 0:
            all_files.extend(idlprocessor.process())

        return all_files
