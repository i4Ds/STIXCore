STIXCore util
*************

The ``util`` submodule contains general purpose code or code that deoesn't belong anywhere else
specifically.

.. automodapi:: stixcore.util

.. automodapi:: stixcore.util.logging

stix-pipline CLI
================

usage: stix-pipline [-h] [-t TM_DIR] [-f FITS_DIR] [-s SPICE_DIR] [-p SOOP_DIR] [--idl_enabled] [--idl_disabled]
                    [--idl_gsw_path IDL_GSW_PATH] [--idl_batchsize IDL_BATCHSIZE] [--stop_on_error]
                    [--continue_on_error] [-o OUT_FILE] [-l LOG_FILE]
                    [--log_level {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}] [-b {TM,LB,L0,L1,L2,ALL}]
                    [-e {TM,LB,L0,L1,L2,ALL}] [--filter FILTER] [-c [CLEAN]]

Runs the stix pipeline

optional arguments:
  -h, --help            show this help message and exit
  -t TM_DIR, --tm_dir TM_DIR
                        input directory to the (tm xml) files
  -f FITS_DIR, --fits_dir FITS_DIR
                        output directory for the
  -s SPICE_DIR, --spice_dir SPICE_DIR
                        directory to the spice kernels files
  -p SOOP_DIR, --soop_dir SOOP_DIR
                        directory to the SOOP files
  --idl_enabled         IDL is setup to interact with the pipeline
  --idl_disabled        IDL is setup to interact with the pipeline
  --idl_gsw_path IDL_GSW_PATH
                        directory where the IDL gsw is installed
  --idl_batchsize IDL_BATCHSIZE
                        batch size how many TM prodcts batched by the IDL bridge
  --stop_on_error       the pipeline stops on any error
  --continue_on_error   the pipeline reports any error and continouse processing
  -o OUT_FILE, --out_file OUT_FILE
                        file all processed files will be logged into
  -l LOG_FILE, --log_file LOG_FILE
                        a optional file all logging is appended
  --log_level LLEVEL
                        LLEVEL: {CRITICAL,ERROR,WARNING,INFO,DEBUG,NOTSET}
                        the level of logging
  -b LEVEL, --start_level LEVEL
                        LEVEL: {TM,LB,L0,L1,L2,ALL}
                        the processing level where to start
  -e LEVEL, --end_level LEVEL
                        LEVEL: {TM,LB,L0,L1,L2,ALL}
                        the processing level where to stop the pipeline
  --filter FILTER, -F FILTER
                        filter expression applied to all input files example '*sci*.fits'
  -c, --clean
                        clean all files from <fits_dir> first
