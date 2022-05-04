#!/usr/bin/bash
# -*- coding: utf-8 -*-

################################################
#
# this is a webhook file to be placed on pub99 web dir: /var/www/data/end2end/rebuild_hook.py
#
# the cgi will trigger to rebuild the test data for the end2end testing
# it assumes that a dedicated stixcore environment is available at /data/stix/end2end/STIXCore/
#
# the final data is packed into a ZIP and copied to /data/stix/end2end/data/ what will be
# available under http://pub099.cs.technik.fhnw.ch/data/end2end/data/
#
#
################################################


# send out the HTPP header
echo "Content-Type: text/html;charset=utf-8"
echo "Access-Control-Allow-Origin: *"
echo "Cache-Control: no-cache"
echo "Connection: keep-alive"
echo "Pragma: no-cache"
echo "Expires: 0"
echo ""
echo "<h2>end2end Testing rebuild data</h2>"
USER=$(whoami)
echo "running as $USER <br />"

DIR="/data/stix/end2end/STIXCore"

cd $DIR

echo "in dir " ; pwd

echo "<br />Git: "

git fetch
git reset --hard origin/master
git clean -f -d

echo "<br />"

#TODO do some git tag version handling here
#HEAD=$(git rev-parse --short HEAD)
#TAG=$(git describe --exact-match --tags)

TAG="head"
ZIPPATH="/data/stix/end2end/data/$TAG.zip"
DATAPATH="/data/stix/end2end/STIXCore/stixcore/data/test/products/end2end"

echo  "clean the old data<br />"
rm -rv $DATAPATH

echo "Recreate the test files at $ZIPPATH and pack them afterwards to: $DATAPATH"

CMD="$DIR/venv/bin/python"
SCRIPT="$DIR/stixcore/util/scripts/end2end_testing.py"

echo "<br />cmd# $CMD $SCRIPT $ZIPPATH $DATAPATH<br />"
nohup $CMD $SCRIPT $ZIPPATH $DATAPATH &>/dev/null &

echo "<h2>all Done zip will be awailable soon</h2>"
