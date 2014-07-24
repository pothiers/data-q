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
PATH=$dir:$PATH

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
    dataq_cli.py  --clear --action off --summary
    test_feeder.py q1.dat 
    dataq_cli.py  --list active --summary
}

function load () {
    dataq_cli.py  --load q2.dat --summary
    dataq_cli.py  --list active --summary
}


###
##############################################################################


echo ""
echo "Starting tests in \"$dir\" ..."
echo ""
echo ""

testCommand feed1 "feed 2>&1" "^\#" n

testCommand dump1 "dataq_cli.py --dump q1.dump --summary" "^\#" n
testOutput out1 q1.dump '^\#' n

testCommand load1 "load 2>&1" "^\#" n

###########################################
echo "WARNING: ignoring remainder of tests"
exit $return_code
###########################################

testCommand load1_1 "test-1.sh 2>&1" "^\#" n
testOutput out1 test-1.out '^\#' n

testCommand advance1_1 "test-2.sh 2>&1" "^\#" n
testOutput out2 test-2.out '^\#' n




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

