#!/bin/bash -e
#
# PURPOSE: <one line description>
#
# EXAMPLE:
#


echo "######################################################################"
echo "##### Basic socket read, load, dump"
dataq_cli.py  --clear --action off --summary
test_feeder.py q1.dat 
dataq_cli.py  --list active --summary
dataq_cli.py  --dump q1.out --summary
cat q1.out 
dataq_cli.py  --load q2.dat --summary
dataq_cli.py  --list active --summary
