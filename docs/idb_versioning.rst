IDB Versioning
==============

After a new IDB Version has been released it should be added to stixcore. All available IDB versions to stixcore are managed in the file `stixcore/data/idb/idbVersionHistory.json`

Follow the instructions to publish a new IDB `2.26.36` version

* Download the released new idb version as zip file from: `https://github.com/i4Ds/STIX-IDB/tags` (https://github.com/i4Ds/STIX-IDB/releases/download/v2.26.35/STIX-IDB-2.26.36.zip)
* unzip locally and create following folder structure:

.. code-block:: bash

    > v2.26.36
        > STIX-IDB-2.26.36
            idb
            README.md
            ...

* remove the `*.mdb` file (possible licensing conflicts)
* zip the folder structure into `v2.26.36.raw.zip`
* upload `v2.26.36.raw.zip` to `https://pub099.cs.technik.fhnw.ch/data/idb/` (`/var/www/data/idb/v2.26.36.raw.zip`)
* compile the raw IDB into a sqlite file (this will also inject some raw 2 engeneering paramaters)

.. code-block:: python

    from stixcore.idb.manager import IDBManager
    IDBManager.instance.compile_version("2.26.36")

* zip the `stixcore/data/idb/v2.26.36` folder into `v2.26.36.zip` >> `zip -r v2.26.37.zip v2.26.37`
* upload `v2.26.36.zip` to `https://pub099.cs.technik.fhnw.ch/data/idb/` (`/var/www/data/idb/v2.26.36.zip`)
* add a new entry to `stixcore/data/idb/idbVersionHistory.json`
    * use https://pub023.cs.technik.fhnw.ch/request/time-conversion/scet2utc
    * or https://pub023.cs.technik.fhnw.ch/request/time-conversion/utc2scet/2021-12-09T00:00:00

.. code-block::

    [...
        {
            "version":"2.26.36",
            "aswVersion":???,
            "validityPeriodUTC":["2020-05-18T09:12:54.151", "2021-06-23T14:38:41.609"],
            "validityPeriodOBT":[{"coarse":643108360, "fine":0}, {"coarse":677774250, "fine":0}]
        },
    ...]

* push the latest updates of the `idbVersionHistory.json`
* after pull stixcore and run the published version `2.26.36` will be automaticaly downlaoded and installed.
