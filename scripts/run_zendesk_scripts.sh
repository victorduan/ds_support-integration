#!/bin/bash

SCRIPT_DIR=/ebs/support-integration/datasift

cd $SCRIPT_DIR

echo
echo "******************************"
echo "Pulling Zendesk Ticket metrics"
echo "******************************"
time ${SCRIPT_DIR}/Z2M-01-pull-tickets.py