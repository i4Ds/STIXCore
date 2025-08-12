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
    - `/data/stix/SOC/incoming` : incoming TM data - entry point of the processing pipeline
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
*/15 * * * * rsync -av stixcore@147.86.8.26:'/home/solmoc/from_edds/tm/*PktTmRaw*.xml' /data/stix/SOLSOC/from_edds/tm/incoming > /dev/null
```

The very latest SPICE kernels are required for accurate pointing information of the satellite.
We may receive TM files via GFTS that contain data for times that are not available in the latest SPICE kernels, as the latest SPICE kernels have not yet been updated and delivered.
Therefore, a stage and delay step is added via cron (every 30 min) that only publishes files older than 2 days (TBC) for processing by the automated pipeline.

```
# move the TM data from the wait into the processing folder - that will trigger the pipeline to "get started": the wait period is meanwhile short
*/30 * * * * find /data/stix/SOLSOC/from_edds/tm/incoming/ -type f -mmin +2 -exec rsync -a {} /data/stix/SOLSOC/from_edds/tm/processing/ \;
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

finally append the latest unzipped files to the processed_files.txt files

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

Some data products are not generated event based on incoming new TM data but once each day. This dayli pipeline reads and writes (also log data) into the same directories as the event base pipeline. Also the generated FITS files might get picked up for publishing to to SOAR later on.

```
# run the daily pipeline
0 8 * * * cd /home/stixcore/STIXCore/ && /home/stixcore/STIXCore/venv/bin/python /home/stixcore/STIXCore/stixcore/processing/pipeline_daily.py
```

IDL - Interactive Data Language
-------------------------------

needs to be installed and licensed available for all users.

In case of problems like: 'CLLFloatingLicenseInitialize failed to start the license thread':

On start, the IDL binary uses the licence library to create some files in /tmp, specifically one called /tmp/fne.[long string of apparently random characters] - this is a zero byte file, owned by the person running IDL. It's not deleted on exit. The next person who tries to run IDL will try to write to the same file name, and fail, despite the file being configured with 0777 permissions. Ubuntu defaults to a non-zero (2) value of fs.protected_regular. If one resets it to zero:

```
sudo sysctl fs.protected_regular=0
```

https://github.com/i4Ds/STIX-GSW should be local available (gsw_path) and always use 'master'

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


Manually reprocess data
-----------------------

When necessary to reprocess certain data products due to fixed errors or enhanced products the following steps might guide you.

If files already have been delivered to SOAR but a reprocessing is necessary a new version number for the same products fits file has to be issued. The version number can be set global in the BaseProduct.PRODUCT_PROCESSING_VERSION or override fine granulated for each product in the class definition. Always the higher value of PRODUCT_PROCESSING_VERSION will be used in the fits file.

Make sure manual (re)processing scripts and automated pipeline (event based or daily) are not running in parallel especially if they write out to the same directory folder.

To stop daily pipeline edit the stixcore user crontab and remove/uncomment the daily pipeline hook.

To stop the event based TM pipeline first check if no processes are running right now (open files: should be 0) and stop the service.

```
stixcore@pub099:~/STIXCore$
stixcore@pub099:~/STIXCore$ source venv/bin/activate
(venv) stixcore@pub099:~/STIXCore$ stix-pipeline-status -n
2024-11-12T15:50:21Z INFO stixcore.processing.pipeline_status 19: connecting to localhost:12388
2024-11-12T15:50:21Z INFO stixcore.processing.pipeline_status 19: connecting to localhost:12388
open files: 0
(venv) stixcore@pub099:~/STIXCore$
(venv) stixcore@pub099:~/STIXCore$ sudo systemctl stop stix-pipeline.service

```
Also consider to stop the publish to SOAR service in the crontab.

stix-pipline CLI
****************

For manually (re)processing of data products use the stix-pipline CLI:

```(bash)

usage: stix-pipeline-cli [-h] [-t TM_DIR] [-f FITS_DIR] [-s SPICE_DIR] [-S SPICE_FILE] [-p SOOP_DIR] [--idl_enabled] [--idl_disabled] [--idl_gsw_path IDL_GSW_PATH] [--idl_batchsize IDL_BATCHSIZE] [--stop_on_error]
                         [--continue_on_error] [-o OUT_FILE] [-l LOG_FILE] [--log_level {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}] [-b {TM,LB,L0,L1,L2,ALL}] [-e {TM,LB,L0,L1,L2,ALL}] [--filter FILTER]
                         [--input_files INPUT_FILES] [-c [CLEAN]] [-r RID_LUT_FILE] [--update_rid_lut]

stix pipeline processing

optional arguments:
  -h, --help            show this help message and exit
  -t TM_DIR, --tm_dir TM_DIR
                        input directory to the (tm xml) files
  -f FITS_DIR, --fits_dir FITS_DIR
                        output directory for the
  -s SPICE_DIR, --spice_dir SPICE_DIR
                        directory to the spice kernels files
  -S SPICE_FILE, --spice_file SPICE_FILE
                        path to the spice meta kernel
  -p SOOP_DIR, --soop_dir SOOP_DIR
                        directory to the SOOP files
  --idl_enabled         IDL is setup to interact with the pipeline
  --idl_disabled        IDL is setup to interact with the pipeline
  --idl_gsw_path IDL_GSW_PATH
                        directory where the IDL gsw is installed
  --idl_batchsize IDL_BATCHSIZE
                        batch size how many TM products batched by the IDL bridge
  --stop_on_error       the pipeline stops on any error
  --continue_on_error   the pipeline reports any error and continouse processing
  -o OUT_FILE, --out_file OUT_FILE
                        file all processed files will be logged into
  -l LOG_FILE, --log_file LOG_FILE
                        a optional file all logging is appended
  --log_level {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}
                        the level of logging
  -b {TM,LB,L0,L1,L2,ALL}, --start_level {TM,LB,L0,L1,L2,ALL}
                        the processing level where to start
  -e {TM,LB,L0,L1,L2,ALL}, --end_level {TM,LB,L0,L1,L2,ALL}
                        the processing level where to stop the pipeline
  --filter FILTER, -F FILTER
                        filter expression applied to all input files example '*sci*.fits'
  --input_files INPUT_FILES, -i INPUT_FILES
                        input txt file with list af absolute paths of files to process
  -c [CLEAN], --clean [CLEAN]
                        clean all files from <fits_dir> first
  -r RID_LUT_FILE, --rid_lut_file RID_LUT_FILE
                        Path to the rid LUT file
  --update_rid_lut      update rid lut file before publishing

```

Make sure that you use a proper output directory (-f). If you write into a directory structure with existing FITS files it is not for sure that existing files gets override as the FITS writers will merge data into existing (same) files. Ovoid this with a other version number or a new output directory or use the --clean option with care.
