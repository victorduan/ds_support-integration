#!/usr/bin/python

"""
This script is intended to update several custom fields 
on the Account, Lead and Contact objects in SFDC. The 
name of each respective owner is pulled and the custom
free-text field is updated. 

"""

import sys
import datetime
import config
import logging
from env import MySqlTask
from env import SalesforceTask

# SFDC Fields
accountOwnerField = 'Account_Owner_Name__c'
leadOwnerField = 'Lead_Owner_Name__c'
contactOwnerField = 'Contact_Owner_Name__c'
accountSubscriptionField = 'Subscription_Plan__c'
sf_batchSize = 200

# Start logging
if config.logLevel == "info":
	logHandler = logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.INFO, filename=config.logFile)
else:
	logHandler = logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.DEBUG, filename=config.logFile)


def processQueryResults(fieldName, data):
	users = {}
	for user in data:
		if user['Owner']['Name'] != user[fieldName]:
			users[user['Id']] = user['Owner']['Name']

	return users

if __name__ == "__main__":

	try:
		# Create object for internal database methods (mySQL)
		mysqlDb = MySqlTask(config.mysql_username, config.mysql_password, config.mysql_host, config.mysql_database)

		# Create object for Salesforce methods
		sfdc = SalesforceTask(config.sfUser, config.sfPass, config.sfApiToken)

		logging.info("Logging into SFDC...")

		######################## SFDC Accounts ########################
		# Get the last modified timestamp
		sfdcLastModified = sfdc.sfdc_timestamp()

		# Grab the last time Accounts were successfully updated
		scriptLastRun = mysqlDb.pull_job_timestamp("SFDC_ACCOUNT_OWNERS_LM")

		query = "SELECT Id, Name, OwnerId, Owner.Name, Account_Owner_Name__c FROM Account WHERE LastModifiedDate > {0}".format(scriptLastRun)
		results = sfdc.sfdc_query(query)
		accountUpdates = processQueryResults(accountOwnerField, results['results'])

		if len(accountUpdates):
			print "Processing {0} Accounts".format(len(accountUpdates))
			sfdc.update_sfdc_object('Account', accountOwnerField, accountUpdates, sf_batchSize)

		mysqlDb.update_job_timestamp("SFDC_ACCOUNT_OWNERS_LM", sfdcLastModified)

		######################## SFDC Leads ########################
		# Get the last modified timestamp
		sfdcLastModified = sfdc.sfdc_timestamp()

		# Grab the last time Leads were successfully updated
		scriptLastRun = mysqlDb.pull_job_timestamp("SFDC_LEAD_OWNERS_LM")

		query = "SELECT Id, Name, OwnerId, Owner.Name, Lead_Owner_Name__c FROM Lead WHERE LastModifiedDate > {0}".format(scriptLastRun)
		results = sfdc.sfdc_query(query)
		leadUpdates = processQueryResults(leadOwnerField, results['results'])

		if len(leadUpdates):
			print "Processing {0} Leads".format(len(leadUpdates))
			sfdc.update_sfdc_object('Lead', leadOwnerField, leadUpdates, sf_batchSize)

		mysqlDb.update_job_timestamp("SFDC_LEAD_OWNERS_LM", sfdcLastModified)

		######################## SFDC Contacts ########################
		# Get the last modified timestamp
		sfdcLastModified = sfdc.sfdc_timestamp()

		# Grab the last time Contacts were successfully updated
		scriptLastRun = mysqlDb.pull_job_timestamp("SFDC_CONTACT_OWNERS_LM")

		query = "SELECT Id, Name, OwnerId, Owner.Name, Contact_Owner_Name__c FROM Contact WHERE LastModifiedDate > {0}".format(scriptLastRun)
		results = sfdc.sfdc_query(query)

		contactUpdates = processQueryResults(contactOwnerField, results['results'])

		if len(contactUpdates):
			print "Processing {0} Contacts".format(len(contactUpdates))
			sfdc.update_sfdc_object('Contact', contactOwnerField, contactUpdates, sf_batchSize)

		mysqlDb.update_job_timestamp("SFDC_CONTACT_OWNERS_LM", sfdcLastModified)

	except Exception, err:
		logging.exception("Exception in: " + str(sys.argv[0]))
		sys.exit()