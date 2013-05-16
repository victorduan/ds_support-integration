#!/bin/bash

SCRIPT_DIR=/ebs/support-integration/ds-support-integration

cd $SCRIPT_DIR

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
