#!/bin/bash -e
# Time-stamp: <07-23-2014 14:50:04 pothiers /home/pothiers/sandbox/data-q/test-1.sh>
#
# PURPOSE: <one line description>
#
# EXAMPLE:
#
# AUTHORS: S.Pothier


echo "######################################################################"
echo "##### Basic socket read, load, dump"
dataq_cli.py  --clear --action off --summary
test_feeder.py q1.dat 
dataq_cli.py  --list active --summary
dataq_cli.py  --dump q1.out --summary
cat q1.out 
dataq_cli.py  --load q2.dat --summary
dataq_cli.py  --list active --summary
