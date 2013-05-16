#!/bin/bash

SCRIPT_DIR=/ebs/support-integration/ds-support-integration

cd $SCRIPT_DIR

echo
echo "*******************************"
echo "Updating Salesforce Owner Names"
echo "*******************************"
time ${SCRIPT_DIR}/S2S-01-owner-names-sync.py
