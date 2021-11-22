import re
import tempfile
from pathlib import Path
from collections import defaultdict

import dominate
import numpy as np
from dominate.tags import (
    a,
    b,
    div,
    h1,
    h2,
    h3,
    h4,
    h5,
    li,
    span,
    table,
    tbody,
    td,
    th,
    thead,
    tr,
    ul,
)

from astropy.io import fits

from stixcore.data.test import test_data
from stixcore.idb.manager import IDBManager
from stixcore.products.product import Product, read_qtable

name_counter = defaultdict(int)

IDB = IDBManager(test_data.idb.DIR).get_idb("2.26.34")


def mtable(columns, data, tclass):
    t = table(cls=tclass, style="width: 100%; table-layout:fixed;")
    _tr = tr()
    for c, style in columns:
        _tr.add(th(c, style=style))
    t.add(thead(_tr))
    tb = tbody()
    for r in data:
        _tr = tr()
        for i, c in enumerate(r):
            _tr.add(td(c, style=columns[i][1]))
        tb.add(_tr)

    t.add(tb)
    return t


def header2table(header):
    headerdata = np.vstack((np.array(list(header.keys())),
                            np.array(list(header.comments)),
                            np.array(list(header.values())))).transpose()
    return mtable([("Name", "width: 20%"),
                   ("Description", "width: 25%"),
                   ("Value example", "width: 50%; overflow:hidden; white-space:nowrap;")
                   ], headerdata, "cheader")


def myunit(unit):
    if unit is None:
        return "-"
    return str(unit)


def mytype(dtype):
    o = dtype.name
    u = re.sub('[0-9]+', '', o)
    u = u.replace("bytes", "str")
    return f"{u}"


def mydim(dim, n):
    o = str(dim)
    u = o.replace("(1, ", "(")
    u = u.replace(f"({n},", "(N,")
    u = u.replace("(N,)", "(N)")
    return f"{u}"


def data2table(data):
    global name_counter
    des = list()
    for colname in sorted(data.columns):
        c = data[colname]

        desc = "TBD"

        if hasattr(c, "meta") and "NIXS" in c.meta and isinstance(c.meta["NIXS"], str):
            desc = "TBC: " + IDB.get_parameter_description(c.meta["NIXS"])
        des.append((colname, mytype(c.dtype), myunit(c.unit), mydim(c.shape, len(c)), desc))
        name_counter[colname] += 1

    return mtable([("Name", "width: 30%"),
                   ("Type", "width: 10%"),
                   ("Unit", "width: 10%"),
                   ("Dims.", "width: 10%"),
                   ("Description", "width: 40%")], des, "cdata")


def mydescriptor(prod):
    return f"stix-{prod.type}-{prod.name}"


def mydescription(prod):
    doc = [l.strip() for l in prod.__doc__.split('\n') if len(l.strip()) > 1]
    return doc[0] if len(doc) > 0 else "TBD"


def myfreefield(prod):
    return "Request ID" if isinstance(prod, ScienceProduct) else "None"


def myfilecadence(prod):
    return "One file per request" if isinstance(prod, ScienceProduct) else "Daily file"


def product(file_in):
    file, remote = file_in
    prod = Product(file)
    with div() as di:
        h4(f"{type(prod).__name__}")
        with ul():
            with li():
                b("Description: ")
                span(mydescription(prod))
            with li():
                b("Descriptor: ")
                span(mydescriptor(prod))
            with li():
                b("Free field: ")
                span(myfreefield(prod))
            with li():
                b("Level: ")
                span(prod.level)
            with li():
                b("File cadence: ")
                span(myfilecadence(prod))
            with li():
                b("Download example: ")
                a(remote, href=remote)

        h5("PRIMARY Header")
        hdul = fits.open(file)
        header2table(hdul["PRIMARY"].header)
        for extname in ["DATA", "CONTROL", "ENERGIES", "IDB_VERSIONS"]:
            try:
                data = read_qtable(file, hdu=extname, hdul=hdul)
                h5(f"Extension: '{extname}'")
                # header2table(hdu[extname].header)
                data2table(data)
            except KeyError:
                pass

        return((prod.level, prod.type, di))


typenames = {"sci": "Science Data",
             "hk": "Housekeeping Data",
             "ql": "Quicklook Data",
             "aux": "Auxilary Data",
             "cal": "Calibration"}

collector = defaultdict(lambda: defaultdict(list))

doc = dominate.document(title='STIX DPDD')

files = [  # L0
           # science
    "L0/21/6/20/solo_L0_stix-sci-xray-rpd-75684608_0640351612f58982-0640351792f58982_V01_50159.fits", # noqa
    "L0/21/6/21/solo_L0_stix-sci-xray-cpd-1267541264_0658960590f39321-0658961099f58980_V01_51710.fits", # noqa
    "L0/21/6/22/solo_L0_stix-sci-xray-spd-87031826_0642038387f06554-0642038399f06553_V01_50889.fits", # noqa
    "L0/21/6/23/solo_L0_stix-sci-xray-vis-2106280003_0678187308f65535-0678187429f58981_V01_54715.fits", # noqa
    "L0/21/6/42/solo_L0_stix-sci-aspect-burst_0670144561f22085-0670166640f63831_V01.fits", # noqa
    "L0/21/6/24/solo_L0_stix-sci-xray-spec-84794884_0641690988f00000-0641692808f00000_V01_51091.fits", # noqa
    # QL
    "L0/21/6/31/solo_L0_stix-ql-background_0668822400_V01.fits",
    "L0/21/6/34/solo_L0_stix-ql-flareflag_0684547200_V01.fits",
    "L0/21/6/30/solo_L0_stix-ql-lightcurve_0684892800_V01.fits",
    "L0/21/6/33/solo_L0_stix-ql-variance_0687484800_V01.fits",
    "L0/21/6/32/solo_L0_stix-ql-spectra_0680400000_V01.fits",
    "L0/21/6/41/solo_L0_stix-cal-energy_0683856000_V01.fits",
    # HK
    "L0/3/25/2/solo_L0_stix-hk-maxi_0647913600_V01.fits",
    "L0/3/25/1/solo_L0_stix-hk-mini_0643507200_V01.fits",

    # L1
    # science
    "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-rpd-2106280010_20210628T092300_20210628T092501_V01_54759.fits", # noqa
    "L1/2021/04/14/SCI/solo_L1_stix-sci-xray-cpd-2104140010_20210414T050020_20210414T050810_V01_53728.fits", # noqa
    "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-scpd-2106280006_20210628T092300_20210628T092501_V01_54720.fits", # noqa
    "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-vis-2106280004_20210628T092300_20210628T092501_V01_54716.fits", # noqa
    "L1/2021/10/13/SCI/solo_L1_stix-sci-aspect-burst_20211013T034957_20211013T092445_V01.fits", # noqa
    "L1/2021/04/14/SCI/solo_L1_stix-sci-xray-spec-2104140004_20210414T035401_20210414T041443_V01_53654.fits", # noqa
    # QL
    "L1/2020/06/16/QL/solo_L1_stix-ql-background_20200616_V01.fits",
    "L1/2020/06/16/QL/solo_L1_stix-ql-flareflag_20200616_V01.fits",
    "L1/2020/06/16/QL/solo_L1_stix-ql-lightcurve_20200616_V01.fits",
    "L1/2020/06/16/QL/solo_L1_stix-ql-variance_20200616_V01.fits",
    "L1/2021/11/16/QL/solo_L1_stix-ql-spectra_20211116_V01.fits",
    "L1/2021/11/16/CAL/solo_L1_stix-cal-energy_20211116_V01.fits",
    # HK
    "L1/2020/06/16/HK/solo_L1_stix-hk-maxi_20200616_V01.fits",
    "L1/2021/09/20/HK/solo_L1_stix-hk-mini_20210920_V01.fits"]

# files = ["http://pub099.cs.technik.fhnw.ch/data/fits_test/" + x for x in files]
# files = ["/home/shane/fits_test/" + x for x in files]
files = ["D:/stixcore_ddpd/" + Path(x).name for x in files]

with tempfile.TemporaryDirectory() as tempdir:
    temppath = Path(tempdir)
    for f in files:
        # lf = temppath / Path(f).name
        # urllib.request.urlretrieve(f, lf)
        # print(f"Download: {f} to {lf}")
        lf = f
        l, t, pr = product(lf)
        collector[l][t].append(pr)

    doc.add(h1("Data Product Description"))

    for level, types in sorted(collector.items()):
        doc.add(h2(level))
        for t, pr in sorted(types.items()):
            doc.add(h3(typenames[t]))
            doc.add(pr)

    # print(doc)

    name_counter = {k: v for k, v in sorted(name_counter.items(),
                                            key=lambda item: item[1], reverse=True)}

    for k, v in name_counter.items():
        print(f"{v}\t{k}\t")

    with open("pdpp.html", "w") as fd:
        fd.write(doc.render(xhtml=True))
