#!/usr/bin/python

import sys
import logging
import config
from env import MySqlTask
from env import SalesforceTask
from env import ZendeskTask

if config.logLevel == "info":
	logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.INFO, filename=config.logFile)
else:
	logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.DEBUG, filename=config.logFile)


if __name__ == "__main__":
	# Create object for internal database methods (mySQL)
	mysqlDb = MySqlTask(config.mysql_username, config.mysql_password, config.mysql_host, config.mysql_database)

	# Create object for Salesforce methods
	sfdc = SalesforceTask(config.sfUser, config.sfPass, config.sfApiToken)

	# Create object for Zendesk methods
	zd = ZendeskTask(config.zenURL, config.zenAgent, config.zenPass, config.zenToken)

	### Pull SFDC Contact Info to Zendesk ###
	try:
		sfdcLastModified = sfdc.sfdc_timestamp()
		logging.info("Current Salesforce Timestamp: {0}".format(sfdcLastModified))
	except Exception, err:
		logging.error(err)
		sys.exit()

	# Get the last modified timestamp from internal database
	startTime = mysqlDb.pull_job_timestamp('SFDC_CONTACT_LM_PULL')
	logging.info("Pulling an internal start time of {0}".format(startTime))

	##### Extract modified SFDC Contacts#####
	logging.info("Pulling Authorized Support Contacts from Salesforce")
	sfdcQuery = "SELECT Email FROM Contact WHERE Authorized_Support_Contact__c = True AND LastModifiedDate > {0}".format(startTime)
	sfdcResults = sfdc.sfdc_query(sfdcQuery)

	for contact in sfdcResults:
		email = contact['Email']
		z = zd.search_by_email(email)
		if z['count']: print z
		else: print "Did not find a matching email: {0}".format(email)