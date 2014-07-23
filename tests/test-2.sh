#!/bin/bash -e
# Time-stamp: <07-23-2014 14:50:39 pothiers /home/pothiers/sandbox/data-q/test-2.sh>
#
# PURPOSE: <one line description>
#
# EXAMPLE:
#
# AUTHORS: S.Pothier


echo "######################################################################"
echo "##### advance, do actions"
dataq_cli.py  --advance 18ea6218a4a8bdc38f52e5466e31973d f45cc4c647913bc6e22df280c733758e --summary
dataq_cli.py  --list active
dataq_cli.py  --action on --summary 
dataq_cli.py  --list active
