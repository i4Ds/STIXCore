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

CRON Processes
--------------

`crontab -e`

Sync TM files
*************

SOLMOC TM data is synced from pub026 via rsync by cron every 15min.



```
# sync TM data from GFTS to local "wait for processing" folder
*/15 * * * * rsync -av stixcore@147.86.8.26:'/home/solmoc/from_edds/tm/*PktTmRaw*.xml' /data/stix/SOLSOC/from_edds/tm/incomming > /dev/null
```

The very latest SPICE kernels are required for accurate pointing information of the satellite.
We may receive TM files via GFTS that contain data for times that are not available in the latest SPICE kernels, as the latest SPICE kernels have not yet been updated and delivered.
Therefore, a stage and delay step is added via cron (every 30 min) that only publishes files older than 2 days (TBC) for processing by the automated pipeline.

```
# move the TM data from the wait into the processing folder - that will trigger the pipeline to "get started": the wait period is meanwhile short
*/30 * * * * find /data/stix/SOLSOC/from_edds/tm/incomming/ -type f -mmin +2 -exec rsync -a {} /data/stix/SOLSOC/from_edds/tm/processing/ \;
```

Sync STIX-CONF repo in all used instances
*****************************************

```
# sync the STIX-CONF to the latest release update version number
# in the publix www folder
10 */4 * * * cd /var/www/data/STIX-CONF && git fetch --tags && latestTag=$(git describe --tags `git rev-list --tags --max-count=1`) && git checkout $latestTag; echo $latestTag > VERSION.TXT
# but also in the Pipeline setup once a day
# had to be copied into the venv folder as well to get activated
20 22 * * * cd /home/stixcore/STIXCore/stixcore/config/data/common/  && git fetch --tags && latestTag=$(git describe --tags `git rev-list --tags --max-count=1`) && git checkout $latestTag; echo $latestTag > VERSION.TXT && cp -r /home/stixcore/STIXCore/stixcore/config/data/common /home/stixcore/STIXCore/venv/lib/python3.9/site-packages/stixcore/config/data/
```

Sync FITS products with Platform (stix data center)
***************************************************

```
# sync current fits to NAS (connection to Platform)
0 */6 * * * sudo rsync -avLo --delete /var/www/data/fits/ /mnt/nas05/stix/stixcore_fits/
```

Sync SPICE kernels and SOOP data
********************************

Spice kernels and SOOP data are syned from GFTS / pub026 via rsync by cron every 15min.

```
rsync -av stixcore@147.86.8.26:/home/solsoc/from_soc /data/stix/SOLSOC/
```

afterwards unprocessed kernel zip files will be extracted to `/data/stix/spice`

```
find /data/stix/SOLSOC/from_soc/ -name "solo_ANC*20*zip" | grep -vFf  processed_files.txt  | xargs -i /usr/bin/unzip -o {} -d /data/stix/spice/
```

finaly append the latest unziped files to the processed_files.txt files

```
find /data/stix/SOLSOC/from_soc/ -name "solo_ANC*20*zip" | grep -vFf  processed_files.txt >> processed_files.txt
```

all together in the cron job:

```
# sync data like spice kernels, soops
*/15 * * * * rsync -av stixcore@147.86.8.26:/home/solsoc/from_soc /data/stix/SOLSOC/ > /dev/null ; find /data/stix/SOLSOC/from_soc/ -name "solo_ANC*20*zip" | grep -vFf  /data/stix/SOLSOC/from_soc/processed_files.txt  | xargs -i /usr/bin/unzip -o {} -d /data/stix/spice/ ; find /data/stix/SOLSOC/from_soc/ -name "solo_ANC*20*zip" | grep -vFf  /data/stix/SOLSOC/from_soc/processed_files.txt >> /data/stix/SOLSOC/from_soc/processed_files.txt
```

Run publish to SOAR
*******************

processed/generated files by the pipeline should be published to ESA on a regular base. This is done by the `stixcore/processing/publish.py` setup a job in the crontab


without provided arguments the default values from `stixcore.ini` are used

```
usage: publish.py [-h] [-t TARGET_DIR] [-T TARGET_HOST] [-s SAME_ESA_NAME_DIR] [-d DB_FILE] [-w WAITING_PERIOD]
                  [-v INCLUDE_VERSIONS] [-l INCLUDE_LEVELS] [-p INCLUDE_PRODUCTS] [-f FITS_DIR]

optional arguments:
  -h, --help            show this help message and exit
  -t TARGET_DIR, --target_dir TARGET_DIR
                        target directory where fits files should be copied to (default:
                        /data/stix/out/test/esa)
  -T TARGET_HOST, --target_host TARGET_HOST
                        target host server where fits files should be copied to
                        (default: localhost)
  -s SAME_ESA_NAME_DIR, --same_esa_name_dir SAME_ESA_NAME_DIR
                        target directory where fits files should be copied to if there
                        are any naming conflicts with already published files (default:
                        /data/stix/out/test/esa_conflicts)
  -r RID_LUT_FILE, --rid_lut_file RID_LUT_FILE
                        Path to the rid LUT file (default:
                        ./stixcore/data/publish/rid_lut.scv)
  --update_rid_lut      update rid lut file before publishing (default: False)
  --sort_files          should the matched FITS be sorted by name before publishing.
                        (default: False)
  -d DB_FILE, --db_file DB_FILE
                        Path to the history publishing database (default:
                        ./stixcore/data/publish/published.sqlite)
  -w WAITING_PERIOD, --waiting_period WAITING_PERIOD
                        how long to wait after last file modification before publishing
                        (default: 14d)
  -v INCLUDE_VERSIONS, --include_versions INCLUDE_VERSIONS
                        what versions should be published (default: *)
  -l INCLUDE_LEVELS, --include_levels INCLUDE_LEVELS
                        what levels should be published (default: L0, L1, L2)
  -p INCLUDE_PRODUCTS, --include_products INCLUDE_PRODUCTS
                        what products should be published (default: ql,hk,sci,aux)
  -f FITS_DIR, --fits_dir FITS_DIR
                        input FITS directory for files to publish (default:
                        /data/stix/out/test/pipeline)
```

```
# run the publish to ESA SOAR once a day
0 5 * * * cd /home/stixcore/STIXCore/ && /home/stixcore/STIXCore/venv/bin/python /home/stixcore/STIXCore/stixcore/processing/publish.py --update_rid_lut
```

Run the pipeline monitor
************************

The event based pipeline (observing incoming telemetry files) gets stuck from time to time. There is a process observing the number of open to process files. If the number of open files is constantly increasing over a longer period a notification mail is send out:

```
# run pipeline monitor task to check for pipeline not stuck
0 */3 * * * cd /home/stixcore/STIXCore/ && /home/stixcore/STIXCore/venv/bin/python /home/stixcore/STIXCore/stixcore/processing/pipeline_monitor.py -s /home/stixcore/monitor_status.json
```

In case of a pipeline stuck restart the event based processing pipeline.

```
# stop the system.d process
sudo systemctl stop stix-pipeline.service

# wait 20sec so that all open sockets also gets closed
# start the process again

sudo systemctl start stix-pipeline.service

```

In order to process all tm files that have not been processed so fare the config parameter start_with_unprocessed should be set to true:

```
[Pipeline]
start_with_unprocessed = True
```

Run the 'daily' pipeline
************************

Some data products are not generated event based on incoming new TM data but once each day. This dayli pipline reads and writes (also log data) into the same directories as the event base pipeline. Also the generated FITS files might get picked up for publishing to to SOAR later on.

```
# run the daily pipeline
0 8 * * * cd /home/stixcore/STIXCore/ && /home/stixcore/STIXCore/venv/bin/python /home/stixcore/STIXCore/stixcore/processing/pipeline_daily.py
```

IDL - Interactive Data Language
-------------------------------

needs to be installed and licensed avaialable for all users.

In case of probles like: 'CLLFloatingLicenseInitialize failed to start the license thread':

On start, the IDL binary uses the licence library to create some files in /tmp, specifically one called /tmp/fne.[long string of apparently random characters] - this is a zero byte file, owned by the person running IDL. It's not deleted on exit. The next person who tries to run IDL will try to write to the same file name, and fail, despite the file being configured with 0777 permissions. Ubuntu defaults to a non-zero (2) value of fs.protected_regular. If one resets it to zero:

```
sudo sysctl fs.protected_regular=0
```

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

* To check the status of the pipeline service

`sudo systemctl status stix-pipeline.service`

* To enable the service on every reboot

`sudo systemctl enable stix-pipeline.service`

* To disable the service on every reboot

`sudo systemctl disable stix-pipeline.service`

* to get/request detailed processing data of the running service you can use a local endpoint

`(venv) stixcore@pub099:~/STIXCore$ stix-pipeline-status -h`

Startup Behavior
*****************

By default the service starts (restart after booting/error) with a search for unprocessed TM files.
This can be disabled with the config `start_with_unprocessed` parameter.

You might toggle the parameter only for manually restarting the service after you have (re)processed some/all TM data in a batch mode. This would allow for a transition from reprocess all at one to daily mode again.

```
[Pipeline]
start_with_unprocessed = False

```
