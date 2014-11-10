#!/bin/bash
# AUTHORS:    S. Pothier
# PURPOSE:    Wrapper for smoke test
# EXAMPLE:
#   loadenv dq
#   $PHOME/tests/smoke.sh
#
# LIMITATIONS:
#   This doesn't startup the required services first!!! The services
#   are given in the "setup" function below.  Haven't decided how I
#   want to handle the case of running smoke when services may or may
#   not be already running.
#
#   There is asynchronous behavior in this stuff.  Testing such is
#   problematic.  
#


file=$0
dir=`dirname $file`
origdir=`pwd`
cd $dir
dir=`pwd`
cd ../dataq
srcdir=`pwd`
cd $dir

PATH=$srcdir:$dir:$PATH

source smoke-lib.sh
return_code=0
SMOKEOUT="README-smoke-results.txt"


##############################################################################
### Test sequence functions
function setup () {
    redis-server &
    dataq_push_svc.py --loglevel DEBUG &    
    dataq_pop_svc.py --loglevel DEBUG &    
}

function feed () {
    dataq_cli.py  --clear --action on --read on
    dataq_cli.py  --action off --summary
    test_feeder.py q1.dat 
    dataq_cli.py  --list active --summary
}

function dump () {
    dataq_cli.py --dump q1.dump --summary
}

function load () {
    dataq_cli.py  --load q2.dat --summary
    dataq_cli.py  --list active --summary
}

function advance() {
    dataq_cli.py  --advance 18ea6218a4a8bdc38f52e5466e31973d f45cc4c647913bc6e22df280c733758e --summary
    dataq_cli.py  --list active
}

function actions() {
    dataq_cli.py  --action on --summary 
    sleep 2 # let queue process (uhg!!)
    dataq_cli.py  --list active
}

function deactivate() {
    dataq_cli.py  --clear --action off --summary
    dataq_cli.py  --load q1.dat 
    dataq_cli.py  --load q2.dat --summary
    dataq_cli.py  --deactivate 5a80736c339faec57dac3ff36563664d 2be92a9f62367b6ee2326887d58e368c --summary
    dataq_cli.py  --list active
    dataq_cli.py  --list inactive
}

function activate() {
    dataq_cli.py  --activate 5a80736c339faec57dac3ff36563664d 8473db06aefc8853aa5da29b645ff865 --summary
    dataq_cli.py  --list active
    dataq_cli.py  --list inactive
    dataq_cli.py  --list records
}


###
##############################################################################


echo ""
echo "Starting tests in \"$dir\" ..."
echo ""
echo ""

testCommand feed1 "feed 2>&1" "^\#" n

testCommand dump1 "dump  2>&1" "^\#" n
testOutput out1 q1.dump '^\#' n

testCommand load1 "load 2>&1" "^\#" n

testCommand advance1 "advance 2>&1" "^\#" n
testCommand actions1 "actions 2>&1" "^\#" n


testCommand deactivate1 "deactivate 2>&1" "^\#" n
testCommand activate1 "activate 2>&1" "^\#" n

###########################################
#!echo "WARNING: ignoring remainder of tests"
#!exit $return_code
###########################################



##############################################################################

rm $SMOKEOUT 2>/dev/null
if [ $return_code -eq 0 ]; then
  echo ""
  echo "ALL smoke tests PASSED ($SMOKEOUT created)"
  echo "All tests passed on " `date` > $SMOKEOUT
else
  echo "Smoke FAILED (no $SMOKEOUT produced)"
fi


# Don't move or remove! 
cd $origdir
exit $return_code
