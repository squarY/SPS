#!/usr/bin/env bash

BASE_DIR="`dirname ${BASH_SOURCE[0]}`/.."
SOLUTION="SEDA"
CLASS_NAME=""

if [ $# -gt 0 ] ; then
  SOLUTION=$1
fi

if [ $SOLUTION = "SEDA"  ] ; then
  CLASS_NAME="com.netflix.sps.stage.singlemachine.Pipeline"
elif [ $SOLUTION = "simple" ] ; then
  CLASS_NAME="com.netflix.sps.bruteforce.SpsApplication"
else
  echo "Usage: start.sh [SEDA|simple]"
fi

java -cp $BASE_DIR/build/libs/sps-all.jar $CLASS_NAME