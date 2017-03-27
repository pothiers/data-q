#!/bin/bash
# Install DATAQ on provisioned Valley or Mountain host
# run as: tada
# run from directory top of installed dataq repo, contianing venv subdir
# Used by puppet


LOG="install.log"
date                              > $LOG
source /opt/tada/venv/bin/activate

dir=`pwd`
#e.g. cd /opt/data-queue
VERSION=`cat dataq/VERSION`
echo "Running install on dir: $dir"

python3 setup.py install --force >> $LOG
echo "Installed DATAQ version: $VERSION" >> $LOG
cat $LOG
