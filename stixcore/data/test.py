"""Wrap all test files into data object."""
import os
from pathlib import Path

data_dir = Path(__file__).parent
test_dir = data_dir.joinpath('test')


class EphemerisTestData:
    def __init__(self, data_dir):
        self.META_KERNEL_POS = data_dir / "ephemeris" / 'mk' / "test_position_20201001_V01.mk"
        self.META_KERNEL_TIME = data_dir / "ephemeris" / 'mk' / "test_time_20201001_V01.mk"
        self.KERNELS_DIR = data_dir / "ephemeris" / "spice" / "kernels"
        self.__doc__ = "\n".join([f'{str(k)}: {repr(v)}\n\n' for k, v in self.__dict__.items()])


class SOOPTestData:
    def __init__(self, data_dir):
        self.DIR = data_dir / "soop"
        self.__doc__ = "\n".join([f'{str(k)}: {repr(v)}\n\n' for k, v in self.__dict__.items()])


class IDBTestData:
    def __init__(self, data_dir):
        self.DIR = data_dir / "idb"
        self.__doc__ = "\n".join([f'{str(k)}: {repr(v)}\n\n' for k, v in self.__dict__.items()])


class IDBTestProduct:
    def __init__(self, data_dir):
        self.DIR = data_dir / "products"
        self.L0_LightCurve_fits = [self.DIR / "solo_L0_stix-ql-lightcurve_0664070400_V01.fits",
                                   self.DIR / "solo_L0_stix-ql-lightcurve_0664156800_V01.fits"]
        self.L1_LightCurve_fits = [self.DIR / "solo_L1_stix-ql-lightcurve_20210117_V01.fits",
                                   self.DIR / "solo_L1_stix-ql-lightcurve_20210116_V01.fits"]
        self.LB_21_6_30_fits = self.DIR / "solo_LB_stix-21-6-30_0664156800_V01.fits"
        self.__doc__ = "\n".join([f'{str(k)}: {repr(v)}\n\n' for k, v in self.__dict__.items()])


class TMTCTestData:
    def __init__(self, data_dir):
        self.TM_DIR = data_dir / "tmtc" / "tm"
        for path, subdirs, files in os.walk(self.TM_DIR):
            for name in files:
                p_name = f"TM_{name.replace('.hex','')}"
                self.__dict__[p_name] = Path(os.path.join(path, name))
        self.__doc__ = "\n".join([f'{str(k)}: {repr(v)}\n\n' for k, v in self.__dict__.items()])


class IOTestData:
    def __init__(self, data_dir):
        self.SOC_DIR = data_dir / "io" / "soc"
        self.TM = self.SOC_DIR / "PktTmRaw.xml"
        self.TC = self.SOC_DIR / "PktTcReport.xml"
        self.HK_MAXI_P1 = self.SOC_DIR / "PktTmHK_MAXI_p1.xml"
        self.HK_MAXI_P2 = self.SOC_DIR / "PktTmHK_MAXI_p2.xml"
        self.__doc__ = "\n".join([f'{str(k)}: {repr(v)}\n\n' for k, v in self.__dict__.items()])


class TestData:
    def __init__(self, data_dir):
        self.ephemeris = EphemerisTestData(data_dir)
        self.idb = IDBTestData(data_dir)
        self.tmtc = TMTCTestData(data_dir)
        self.products = IDBTestProduct(data_dir)
        self.io = IOTestData(data_dir)
        self.soop = SOOPTestData(data_dir)

        self.__doc__ = "\n".join([f"{k}\n******************\n\n{v.__doc__}\n\n\n"
                                  for k, v in self.__dict__.items()])


test_data = TestData(test_dir)

__doc__ = test_data.__doc__

__all__ = ['test_data', 'data_dir', 'test_dir']
