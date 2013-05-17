#!/bin/bash

SCRIPT_DIR=/ebs/support-integration/datasift

cd $SCRIPT_DIR

echo
echo "********************************"
echo "Pulling /usage for list of users"
echo "********************************"
time ${SCRIPT_DIR}/D2M-01-usage-collection.py