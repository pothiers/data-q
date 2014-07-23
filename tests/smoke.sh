#!/bin/bash
# AUTHORS:    S. Pothier
# PURPOSE:    Wrapper for smoke test
# EXAMPLE:
#   loadenv dq
#   $PHOME/tests/smoke.sh
#

file=$0
dir=`dirname $file`
origdir=`pwd`
cd $dir

source smoke-lib.sh
return_code=0
SMOKEOUT="README-smoke-results.txt"


echo ""
echo "Starting tests in \"$dir\" ..."
echo ""
echo ""


#! testCommand conrules1_1 "conrules -p ../python -m jungle-objects.xml jungle.lpc.gz conrules_out.1.lpc.gz 2>&1" "^\#" n
#! lpc_cat conrules_out.1.lpc.gz > conrules_out.1.lpc.xml
#! testOutput  conrules1_2 conrules_out.lpc.xml "^\#" n


testOutput out test-1.out '^\#' n
testOutput out test-2.out '^\#' n


###########################################
#! echo "WARNING: ignoring remainder of tests"
#! exit $return_code
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

