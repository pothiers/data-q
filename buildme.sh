#!/bin/bash -e
# PURPOSE: Build distributions to install elsewhere.
#
# EXAMPLE: 
#
# AUTHORS: S.Pothier

cmd=`basename $0`
SCRIPT=$(readlink -f $0)        # Absolute path to this script
SCRIPTPATH=$(dirname $SCRIPT)   # Absolute path to dir containing this script 

usage="USAGE: $cmd [options] [reportFile]
"

MINARGS=0
if [ $# -lt $MINARGS ]; then
    echo >&2 "ERROR: Wrong number of arguments.  Got $#, expected $MINARGS"
    echo >&2 "$usage"
    exit 2
fi

report=${1:-$HOME/logs/$cmd.report}

##############################################################################

pushd $SCRIPTPATH

# Change version to new user supplied string
#! vers=`grep version= setup.py | awk '{ print  substr(substr($1,1,length($1)-2),10) }'`
vers=`cat dataq/VERSION`
echo "Current version is: $vers"
read -i "$vers" -p "What is the new version? " newvers rem
#! perl -pi.bak -e "s/$vers/$newvers/" setup.py
#! echo "NEW version is: $newvers"
echo $newvers > dataq/VERSION

# do NOT run under venv!
python3 setup.py build bdist --format rpm,gztar

popd

echo "Binary distributions (including rpm) written to 'dist'."
echo "Now would be a good time to run rpms-to-repo.sh"
