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
    - `/data/stix` : data root dir
    - `/data/stix/SOC/` : exchange dir with SOC
    - `/data/stix/SOC/incoming` : incoming TM data - entry point of the processing pipline
    - `/data/stix/SOLSOC/` : exchange dir with SOLSOC
    - `/data/stix/SOLSOC/from_soc` : incoming sync folder from soc
    - `/data/stix/spice` : spice kernel dir synced from GFTS



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

Sync SPICE kernels
------------------

Spice kernels are syned from GFTS / pub026 via rsync by cron every 15min.

`rsync -av stixcore@147.86.8.26:/home/solsoc/from_soc /data/stix/SOLSOC/`

afterwards unprocessed kernel zip files will be extracted to `/data/stix/spice`

`find /data/stix/SOLSOC/from_soc/ -name "solo_ANC*20*zip" | grep -vFf  processed_files.txt  | xargs -i /usr/bin/unzip -o {} -d /data/stix/spice/`

finaly append the latest unziped files to the `processed_files.txt` files

`find /data/stix/SOLSOC/from_soc/ -name "solo_ANC*20*zip" | grep -vFf  processed_files.txt >> processed_files.txt`

all together in the cron job:

`crontab -e`

`*/15 * * * * rsync -av stixcore@147.86.8.26:/home/solsoc/from_soc /data/stix/SOLSOC/ ; find /data/stix/SOLSOC/from_soc/ -name "solo_ANC*20*zip" | grep -vFf  processed_files.txt  | xargs -i /usr/bin/unzip -o {} -d /data/stix/spice/ ; find /data/stix/SOLSOC/from_soc/ -name "solo_ANC*20*zip" | grep -vFf  processed_files.txt >> processed_files.txt`



IDL - Interactive Data Language
-------------------------------

needs to be installed and licensed avaialable for all users.

In case of probles like: 'CLLFloatingLicenseInitialize failed to start the license thread' see here: https://www.l3harrisgeospatial.com/Support/Self-Help-Tools/Help-Articles/Help-Articles-Detail/ArtMID/10220/ArticleID/24045/How-to-resolve-error-%E2%80%9CCLLFloatingLicenseInitialize-failed-to-start-the-license-thread%E2%80%9D (the latest option is doing the trick)

https://github.com/i4Ds/STIX-GSW should be local avaialable (gsw_path) and always use 'master'

enable IDB bridge with entry in stixcore.ini:

```
[IDLBridge]
enabled = True
gsw_path = /opt/STIX-GSW

```

SETUP - Pipeline as systemd service
-----------------------------------

1: copy stixcore/util/scripts/stix-pipeline.service into /etc/systemd/system

`sudo cp stixcore/util/scripts/stix-pipeline.service /etc/systemd/system`

2: update /etc/systemd/system/stix-pipeline.service with log-pathes users if needed

3: Reload the service files to include the new service.

`sudo systemctl daemon-reload`

4: Start the service

`sudo systemctl start stix-pipeline.service`

5: To check the status of the pipeline service

`sudo systemctl status stix-pipeline.service`

6: To enable the service on every reboot

`sudo systemctl enable stix-pipeline.service`

7: To disable the service on every reboot

`sudo systemctl disable stix-pipeline.service`
