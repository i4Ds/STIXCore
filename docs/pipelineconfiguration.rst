Pipeline Configuration
======================

This section documents how the pipeline is set up on a production server.

User
----

`stixcore`

`/home/stixcore` : home dir of user

Locations
---------

* Data
    `/data/stix` : data root dir

    `/data/stix/SOC/` : exchange dir with SOC

    `/data/stix/SOC/incoming` : incoming TM data - entry point of the processing pipline

* Programs / Libs / Dependencies
    `/opt/stixcore` :
    `/opt/STIX-GSW` : stix idl processing library: https://github.com/i4Ds/STIX-GSW
    `/usr/local/ssw` : IDL SolarSoft: https://www.lmsal.com/solarsoft/ssw_setup.html
    `/home/stixcore/astrolib` : GDL astrolib
    `/home/stixcore/.gdl/gdl-startup.pro`: GDL startup script with path to SSW, STIX-GSW, astrolib and homedir `/home/stixcore/`


Sync with SOC
-------------

SOC TM data is synced from pub026 via rsync by cron every 10min.

`crontab -e`

`*/10 * * * * rsync -av stixcore@147.86.8.26:/home/solmoc/from_moc/*  /data/stix/SOC/incoming/ --exclude-from="/data/stix/SOC/processed_files.txt"`

allready procced files should by appended to `/data/stix/SOC/processed_files.txt` and can then be removed from the `/incoming` folder.

GDL - GNU Data Language
-----------------------

Version 0.9.9

Install: https://www.scivision.dev/gdl-setup-with-astronomy-library-open-source-idl-replacement/

start as user `stixcore`

cmd example: `> gdl -e "a = fitstest()"`
