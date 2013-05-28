#!/bin/bash

SCRIPT=$(dirname $(readlink -f $0))
SCRIPT_DIR=`cd $SCRIPT; cd ..; pwd`

echo
echo "******************************"
echo "Pulling Zendesk Ticket metrics"
echo "******************************"
time ${SCRIPT_DIR}/Z2M-01-pull-tickets.py