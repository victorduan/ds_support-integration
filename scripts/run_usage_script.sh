#!/bin/bash

SCRIPT=$(dirname $(readlink -f $0))
SCRIPT_DIR=`cd $SCRIPT; cd ..; pwd`

echo
echo "********************************"
echo "Pulling /usage for list of users"
echo "********************************"
time ${SCRIPT_DIR}/D2M-01-usage-collection.py