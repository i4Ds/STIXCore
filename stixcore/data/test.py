"""Wrap all test files into data object."""
from pathlib import Path

data_dir = Path(__file__).parent
test_dir = data_dir.joinpath('test')


class EphemerisTestData:
    def __init__(self, data_dir):
        self.KERNELS_DIR = data_dir / "ephemeris" / "spice" / "kernels"
        self.__doc__ = "\n".join([f'{str(k)}: {repr(v)}\n\n' for k, v in self.__dict__.items()])


class SOOPTestData:
    def __init__(self, data_dir):
        self.DIR = data_dir / 'soop'
        self.API_RES = data_dir / 'soop' / 'api' / 'soop_api.json'
        self.__doc__ = "\n".join([f'{str(k)}: {repr(v)}\n\n' for k, v in self.__dict__.items()])


class IDBTestData:
    def __init__(self, data_dir):
        self.DIR = data_dir / "idb"
        self.__doc__ = "\n".join([f'{str(k)}: {repr(v)}\n\n' for k, v in self.__dict__.items()])


class IDBTestProduct:
    def __init__(self, data_dir):
        self.DIR = data_dir / "products"
        self.DIREND2END = self.DIR / "end2end"
        self.L0_LightCurve_fits = [self.DIR / "solo_L0_stix-ql-lightcurve_0664070400_V01.fits",
                                   self.DIR / "solo_L0_stix-ql-lightcurve_0664156800_V01.fits"]
        self.L1_LightCurve_fits = [self.DIR / "solo_L1_stix-ql-lightcurve_20210117_V01.fits",
                                   self.DIR / "solo_L1_stix-ql-lightcurve_20210116_V01.fits"]
        self.L1_fits = list(self.DIR.glob('solo_L1_stix-*.fits'))
        self.LB_21_6_30_fits = self.DIR / "solo_LB_stix-21-6-30_0664156800_V01.fits"
        self.LB_21_6_21_fits_scaled = self.DIR / "solo_LB_stix-21-6-21_0000000000-9999999999_V02_2312148821-53879.fits"  # noqa
        self.LB_21_6_24_scale_change = self.DIR / "solo_LB_stix-21-6-24_0000000000-9999999999_V02_2402021788-59493.fits" # noqa
        self.__doc__ = "\n".join([f'{str(k)}: {repr(v)}\n\n' for k, v in self.__dict__.items()])


class TMTCTestData:
    def __init__(self, data_dir):
        self.TM_DIR = data_dir / "tmtc" / "tm"
        for f in self.TM_DIR.rglob("*.hex"):
            p_name = f"TM_{f.name.replace('.hex','')}"
            self.__dict__[p_name] = f
        self.XML_TM = [self.TM_DIR / "Test2.PktTmRaw.xml",
                       self.TM_DIR / "Test0.PktTmRaw.xml",
                       self.TM_DIR / "Test1.PktTmRaw.xml"]
        self.__doc__ = "\n".join([f'{str(k)}: {repr(v)}\n\n' for k, v in self.__dict__.items()])


class IOTestData:
    def __init__(self, data_dir):
        self.SOC_DIR = data_dir / "io" / "soc"
        self.TM = self.SOC_DIR / "PktTmRaw.xml"
        self.TC = self.SOC_DIR / "PktTcReport.xml"
        self.HK_MAXI_P1 = self.SOC_DIR / "PktTmHK_MAXI_p1.xml"
        self.HK_MAXI_P2 = self.SOC_DIR / "PktTmHK_MAXI_p2.xml"
        self.__doc__ = "\n".join([f'{str(k)}: {repr(v)}\n\n' for k, v in self.__dict__.items()])


class RidLutTestData:
    def __init__(self, data_dir):
        self.PUB_DIR = data_dir / "publish"
        self.RID_LUT = self.PUB_DIR / "rid_lut.csv"
        self.RID_LUT_UPDATE = self.PUB_DIR / "update_rid_lut.csv"


class TestData:
    def __init__(self, data_dir):
        self.ephemeris = EphemerisTestData(data_dir)
        self.idb = IDBTestData(data_dir)
        self.tmtc = TMTCTestData(data_dir)
        self.products = IDBTestProduct(data_dir)
        self.io = IOTestData(data_dir)
        self.soop = SOOPTestData(data_dir)
        self.rid_lut = RidLutTestData(data_dir)
        self.ecc = data_dir / "ecc"

        self.__doc__ = "\n".join([f"{k}\n******************\n\n{v.__doc__}\n\n\n"
                                  for k, v in self.__dict__.items()])


test_data = TestData(test_dir)

__doc__ = test_data.__doc__

__all__ = ['test_data', 'data_dir', 'test_dir']
