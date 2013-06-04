#!/usr/bin/python

import sys
import logging
import config
import re
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
	startTime = { "startTime" : mysqlDb.pull_job_timestamp('SFDC_CONTACT_LM_PULL') }
	logging.info("Pulling an internal start time of {0}".format(startTime))

	##### Extract modified SFDC Contacts#####
	logging.info("Pulling Authorized Support Contacts from Salesforce")
	sfdcQuery = """
				SELECT Email, MailingPostalCode, Phone, Authorized_Support_Contact__c 
				FROM Contact 
				WHERE LastModifiedDate > %(startTime)s
				""" % startTime
	sfdcResults = sfdc.sfdc_query(sfdcQuery)

	for contact in sfdcResults['results']:
		data = { }
		data['tags'] = []
		user_id = 0
					
		# Verify the user has already been created in Zendesk
		if contact['Email'] != '':
			email = contact['Email']
			z = zd.search_by_email(email)
			if z['count']:
				user_id = z['users'][0]['id'] 
				data["email"] = email	
			
		# Only update Zendesk if the user already exists	
		if user_id:
			# Process the Postal Code to determine Time Zone
			if contact['MailingPostalCode'] != '':
				postalcode = re.search('^[0-9]{5}', contact['MailingPostalCode'])
				if postalcode is not None: 
					timezone = mysqlDb.get_timezone_by_zip(postalcode.group())
					if timezone != '':
						data["time_zone"] = timezone
					
			# Check to see if the user is an Authorized Contact	
			if contact['Authorized_Support_Contact__c'] == True:
				data['tags'].append("authorized_support_contact")
				
			# Check if phone is populated
			if contact['Phone'] != '':
				data['phone'] = contact['Phone']
				
			try:
				# Update Zendesk
				#update = zd.update_user(user_id, data)
				print update
			except Exception, err:
				logging.exception("Exception updating Zendesk. Error: {0}".format(err))
		
	
	