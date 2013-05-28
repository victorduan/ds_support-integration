#!/bin/bash

SCRIPT=$(dirname $(readlink -f $0))
SCRIPT_DIR=`cd $SCRIPT; cd ..; pwd`

echo
echo "******************************"
echo "Pulling Contact Info From SFDC"
echo "******************************"
time ${SCRIPT_DIR}/S2Z-01-contact-sync.py

echo
echo "*******************************"
echo "Updating Salesforce Owner Names"
echo "*******************************"
time ${SCRIPT_DIR}/S2Z-02-org-sync.py
