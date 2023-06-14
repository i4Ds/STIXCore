"""A helper script to generate the table parts of the DDPD document.

Workflow:

sample LL dada products are in /home/nicky/LL

add all products that should be included into the doc into the main.files list

run the script (best on pub099).
It will generate output in stixcore/util/scripts/ddpd.html

Descriptions are derived from defferent sources doc strings, idb, fits files
last override is from stixcore/data/test/ddpd.in.csv

open the html in word.
run the following VB script to select all tables:

https://www.datanumen.com/blogs/2-ways-apply-style-tables-word-document/

'Select all tables in a Word document.
Sub SelectAllTables()
  Dim objDoc As Document
  Dim objTable As Table

  Application.ScreenUpdating = False

  'Initialization
  Set objDoc = ActiveDocument

  'Set each table in document as a range editable to everyone.
  With objDoc
  For Each objTable In .Tables
    objTable.Range.Editors.Add wdEditorEveryone
  Next
  objDoc.SelectAllEditableRanges wdEditorEveryone
  objDoc.DeleteAllEditableRanges wdEditorEveryone
  Application.ScreenUpdating = True
  End With
End Sub

select a table style.

copy all pages (copy past) into https://v000658.fhnw.ch/index.php/f/3310201

select all heading (each type once) and select "all instances" change the style to the document
heading type (heading1 ... N)

save document as new DDPD revision and publish to CDM

"""

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
from astropy.table.table import Table

from stixcore.data.test import test_data
from stixcore.idb.manager import IDBManager
from stixcore.products.level0.scienceL0 import ScienceProduct
from stixcore.products.product import Product, read_qtable

name_counter = defaultdict(int)

IDB = IDBManager(test_data.idb.DIR).get_idb("2.26.34")
descriptions = Table.read("stixcore/data/test/ddpd.in.csv", format='csv', delimiter="\t")


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
                            np.array([str(v).replace(";", "; ").replace(".SOL.", ".SOL. ")
                                      for v in header.values()]))).transpose()
    return mtable([("Name", "width: 15%"),
                   ("Description", "width: 25%"),
                   ("Value example", "width: 60%")
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


def mydim(dim, n, colname):
    o = str(dim)
    # u = o.replace("(1, ", "(")
    u = str(o)
    u = u.replace(f"({n},", "(")
    u = u.replace("( ", "(")
    u = u.replace("()", "(1)")
    u = u.replace(" ", "")
    # u = u.replace("(N,)", "(N)")
    # u = re.sub(r"\(([0-9]+)", r"(N, \1", u)
    if colname == 'packet':
        u = '(P)'
    return f"Nx{u}"


def data2table(data, level):
    global name_counter
    des = list()
    for colname in sorted(data.columns):
        c = data[colname]

        desc = "TBD"

        if colname in descriptions['name']:
            _desc = str(descriptions['description'][descriptions['name'] == colname][0])
            parts = _desc.split("|")
            if len(parts) > 1:
                _desc = parts[0] if level == "L0" else parts[1]
            if _desc != '--':
                desc = _desc
            else:
                name_counter[colname] += 1
        else:
            name_counter[colname] += 1

        if (desc == "TBD" and hasattr(c, "meta") and
           "NIXS" in c.meta and isinstance(c.meta["NIXS"], str)):
            desc = IDB.get_parameter_description(c.meta["NIXS"])

        des.append((colname, mytype(c.dtype), myunit(c.unit),
                    mydim(c.shape, len(c), colname), desc))

    return mtable([("Name", "width: 30%"),
                   ("Type", "width: 10%"),
                   ("Unit", "width: 10%"),
                   ("Dims.", "width: 10%"),
                   ("Description", "width: 40%")], des, "cdata")


def mydescriptor(prod, sup=0):
    if sup > 0:
        return f"stix-{prod.type}-{prod.name}-sup{sup}"
    return f"stix-{prod.type}-{prod.name}"


def mydescription(prod):
    doc = [ldesc.strip() for ldesc in (prod.__doc__+"\n\n\n").split('\n\n\n')
           if len(ldesc.strip()) > 1]
    return doc[0] if len(doc) > 0 else "TBD"


def myfreefield(prod):
    return "Request ID" if isinstance(prod, ScienceProduct) else "None"


def myfilecadence(prod):
    return "One file per request" if isinstance(prod, ScienceProduct) else "Daily file"


def product(file_in):
    file, remote = file_in
    prod = Product(file)
    with div() as di:
        a(h4(f"{type(prod).__name__}"), id=f"{prod.level}-{prod.type}-{prod.name}")
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
        for extname in ["DATA", "CONTROL", "IDB_VERSIONS", "ENERGIES"]:
            try:
                data = read_qtable(file, hdu=extname, hdul=hdul)
                h5(f"Extension: '{extname}'")
                # header2table(hdu[extname].header)
                data2table(data, prod.level)
            except KeyError:
                pass
        if prod.type == 'sci':
            h5("Supplements")
            with ul():
                with li():
                    b("Description: ")
                    span(f"""For {type(prod).__name__} data products, supplementary data of this type may be available for the same time period.
                          These data originate from the same raw measurements recorded onboard but were downlinked with another configuration by an operator request.
                          Most commonly, the various configuration uses cases are different pixel/detector masks or time/energy binning.
                          Such supplements can contain additional but also (partly) overlapping data compared to the already existing data product of the same time and type (base product).
                          In the FITS "COMMENT" header keyword, you will find a reference to the base product and a brief description of the TM configuration for this product.
                          If supplement data products are available for the same time period, merging this data into the base product for the best scientific approach could be worthwhile.
                          But this merging has to be done manually by the user. Please contact the STIX team if you have any questions.""") # noqa
                with li():
                    b("Descriptors: ")
                    span(mydescriptor(prod, sup=1))
                    span(mydescriptor(prod, sup=2))
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
                    b("Keyword and Extension definition see:")
                    span(a(type(prod).__name__, href=f"#{prod.level}-{prod.type}-{prod.name}"))

        return (prod.level, prod.type, di)


if __name__ == '__main__':

    typenames = {"sci": "Science Data",
                 "hk": "Housekeeping Data",
                 "ql": "Quicklook Data",
                 "aux": "Auxilary Data",
                 "cal": "Calibration"}

    collector = defaultdict(lambda: defaultdict(list))

    doc = dominate.document(title='STIX DPDD')

    files = [  # LL
        # "LL/solo_LL01_stix-ql-lightcurve_0628185012-0628186272_V202203231133.fits",
        # "LL/solo_LL01_stix-ql-flareflag_0628185012-0628186272_V202203231133.fits",
        # L0
        # science
        "L0/21/6/20/solo_L0_stix-sci-xray-rpd_0678187309-0678187429_V01_2106280011-54760.fits", # noqa
        "L0/21/6/21/solo_L0_stix-sci-xray-cpd_0688454771-0688455372_V01_2110250007-65280.fits", # noqa
        "L0/21/6/22/solo_L0_stix-sci-xray-scpd_0642038387-0642038407_V01_0087031810-50884.fits", # noqa
        "L0/21/6/23/solo_L0_stix-sci-xray-vis_0678187308-0678187429_V01_2106280004-54716.fits", # noqa
        "L0/21/6/42/solo_L0_stix-sci-aspect-burst_0687412111-0687419343_V01_2110130059.fits", # noqa
        "L0/21/6/24/solo_L0_stix-sci-xray-spec_0689786926-0689801914_V01_2111090002-50819.fits", # noqa
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
        # CAL
        "L0/21/6/41/solo_L0_stix-cal-energy_0640137600_V01.fits",

        # L1
        # science
        "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-rpd_20210628T092301-20210628T092501_V01_2106280010-54759.fits", # noqa
        "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-cpd_20210628T190505-20210628T191459_V01_2106280009-54755.fits", # noqa
        "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-scpd_20210628T092301-20210628T092502_V01_2106280006-54720.fits", # noqa
        "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-vis_20210628T092301-20210628T092502_V01_2106280004-54716.fits", # noqa
        "L1/2021/10/13/SCI/solo_L1_stix-sci-aspect-burst_20211013T034959-20211013T055031_V01_2110130059.fits", # noqa
        "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-spec_20210628T230132-20210628T234143_V01_2106280041-54988.fits", # noqa
        # QL
        "L1/2020/06/16/QL/solo_L1_stix-ql-background_20200616_V01.fits",
        "L1/2020/06/16/QL/solo_L1_stix-ql-flareflag_20200616_V01.fits",
        "L1/2020/06/16/QL/solo_L1_stix-ql-lightcurve_20200616_V01.fits",
        "L1/2020/06/16/QL/solo_L1_stix-ql-variance_20200616_V01.fits",
        "L1/2021/11/16/QL/solo_L1_stix-ql-spectra_20211116_V01.fits",
        "L1/2021/11/16/CAL/solo_L1_stix-cal-energy_20211116_V01.fits",
        # HK
        "L1/2020/06/16/HK/solo_L1_stix-hk-maxi_20200616_V01.fits",
        "L1/2021/09/20/HK/solo_L1_stix-hk-mini_20210920_V01.fits",
        # CAL
        "L1/2023/02/13/CAL/solo_L1_stix-cal-energy_20230213_V01.fits"
        ]

    remote = ["http://pub099.cs.technik.fhnw.ch/data/fits/" + x for x in files]
    # files = ["/home/shane/fits_test/" + x for x in files]
    files = [("/data/stix/out/fits_v1.0.0/" + x, remote[i]) for i, x in enumerate(files)]

    with tempfile.TemporaryDirectory() as tempdir:
        temppath = Path(tempdir)
        for f in files:
            try:
                # lf = temppath / Path(f).name
                # urllib.request.urlretrieve(f, lf)
                # print(f"Download: {f} to {lf}")
                lf = f
                l, t, pr = product(lf)
                collector[l][t].append(pr)
            except Exception as e:
                print(e)

        doc.add(h1("Data Product Description"))

        for level, types in sorted(collector.items()):
            doc.add(h2(level))
            for t, pr in sorted(types.items()):
                doc.add(h3(typenames[t]))
                doc.add(pr)

        # print(doc)

        name_counter = {k: v for k, v in sorted(name_counter.items(),
                                                key=lambda item: item[1], reverse=True)}

        lutable = Table()
        lutable['counter'] = list(name_counter.values())
        lutable['name'] = list(name_counter.keys())
        lutable['description'] = ""

        print(lutable)

        # from astropy.io import ascii
        # ascii.write(lutable, str(Path(__file__).parent / 'ddpd.out.csv'), overwrite=True,
        #             format='csv', delimiter="\t")

        with open(Path(__file__).parent / "ddpd.html", "w") as fd:
            fd.write(doc.render(xhtml=True))

        print("all done")
