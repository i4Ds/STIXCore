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

* Programs
    `/opt/stixcore` :

Sync with SOC
-------------

SOC TM data is synced from pub026 via rsync by cron every 10min.

`crontab -e`

`*/10 * * * * rsync -av stixcore@147.86.8.26:/home/solmoc/from_moc/*  /data/stix/SOC/incoming/ --exclude-from="/data/stix/SOC/processed_files.txt"`

allready procced files should by appended to `/data/stix/SOC/processed_files.txt` and can then be removed from the `/incoming` folder.
