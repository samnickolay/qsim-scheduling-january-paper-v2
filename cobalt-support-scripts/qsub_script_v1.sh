#!/bin/bash

CWD_PATH=/gpfs/jlse-fs0/users/samnickolay
export PYTHONPATH=$CWD_PATH/cobalt-master/src
export HOST=ubuntu
export PYTHONUNBUFFERED=1
export COBALT_CONFIG_FILES=$CWD_PATH/cobalt-master/src/components/conf/cobalt-gomez.conf
#export COBALT_CONFIG_FILES=$CWD_PATH/cobalt-master/src/components/conf/cobalt.conf
rm -r $CWD_PATH/cobalt-master/src/components/results/*


"$@"

cp $CWD_PATH/automating-script-output-wk*.log $CWD_PATH/cobalt-master/src/components/results/
