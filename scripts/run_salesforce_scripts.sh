#!/bin/bash

SCRIPT=$(dirname $(readlink -f $0))
SCRIPT_DIR=`cd $SCRIPT; cd ..; pwd`

echo
echo "*******************************"
echo "Updating Salesforce Owner Names"
echo "*******************************"
time ${SCRIPT_DIR}/S2S-01-owner-names-sync.py
