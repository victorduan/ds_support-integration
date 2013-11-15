#/usr/bin
# -*- coding: utf-8 -*-

from env import SalesforceTask
from env import MySqlTask
import config
import re
import logging
import sys

if config.logLevel == "info":
	logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.INFO, filename=config.logFile)
else:
	logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.DEBUG, filename=config.logFile)

sfdc = SalesforceTask(config.sfUser, config.sfPass, config.sfApiToken)
mysqlDb = MySqlTask(config.mysql_username, config.mysql_password, config.mysql_host, config.mysql_database)

sfdc_update = {}

# Get the last modified timestamp from internal database
startTime = mysqlDb.pull_job_timestamp('SFDC_DATASIFT_USER_ID')
logging.info("Pulling SFDC_DATASIFT_USER_ID; start time of {0}".format(startTime))

# Pull from SFDC
query = "Select Id, DataSift_UserID__c, Name FROM Account WHERE Account_Status__c = 'Customer' and DataSift_UserID__c != '' and LastModifiedDate > {0}".format(startTime)

results = sfdc.sfdc_query(query)

if results['count'] > 0:
	for account in results['results']:
		if account['Id'] not in sfdc_update:
			sfdc_update[account['Id']] = []
		ids = re.findall(r'[0-9]+', account['DataSift_UserID__c'])
		if len(ids):
			for i in ids:
				# Query Users table
				mquery = "SELECT user_id, email, username FROM users WHERE user_id = '{0}'".format(i)
				mysqlDb.connect()
				r = mysqlDb.select_query(mquery)

				if len(r):
					data = {
						'user_id' 	: r[0][0],
						'email'		: r[0][1],
						'username'	: r[0][2],
						'name'		: account['Name'].decode('utf-8'),
						'accountid' : account['Id']
					}
					print data
					insert = 	"""
								INSERT into account_link 
								(datasift_user_id, datasift_username, datasift_email_address, accountid, accountname)
								VALUES ('%(user_id)s', '%(username)s', '%(email)s', '%(accountid)s', '%(name)s')
								ON DUPLICATE KEY UPDATE datasift_username = '%(username)s', datasift_email_address = '%(email)s', accountid = '%(accountid)s', accountname = '%(name)s' 
							 	""" % data
					logging.debug("MySQL Query: {0}".format(insert.encode('utf-8')))
					mysqlDb.connect()
					mysqlDb.execute_query(insert)
					mysqlDb.commit()

					sfdc_update[account['Id']].append(r[0][2]) # Append the username returned

# Update SFDC with the usernames

if len(sfdc_update):
	data = {}
	for sfdc_id, usernames in sfdc_update.items():
		data[sfdc_id] = ", ".join(usernames)
	sfdc.update_sfdc_object('Account', 'Username_s__c', data)
	logging.info("Finished updating SFDC.")
	try:
		sfdcLastModified = sfdc.sfdc_timestamp()
		logging.info("Current Salesforce Timestamp: {0}".format(sfdcLastModified))
	except Exception, err:
		logging.exception(err)
		sys.exit()
	mysqlDb.update_job_timestamp('SFDC_DATASIFT_USER_ID', sfdcLastModified)
	logging.info("Updating SFDC_DATASIFT_USER_ID timestamp.")
