#!/usr/bin/python

import sys
import logging
import config
import json
import string
import time
from env import MySqlTask
from env import SalesforceTask
from env import ZendeskTask

if config.logLevel == "info":
	logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.INFO, filename=config.logFile)
else:
	logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.DEBUG, filename=config.logFile)


def ProcessSfdcAccounts(results):

	processedResults = []

	for record in results:
		# SAMPLE RECORD
		#	{
		#		'Name': 'Unified Social', 
		#		'Named_Support_Engineer__c': '', 
		#		'Technical_Account_Manager__r': 
		#			{
		#				'type': 'User', 
		#				'Id': '', 
		#				'Name': 'Steve Lee'
		#			}, 
		#		'Subscription_Plan__c': 'Bronze', 
		#		'Zendesk__Domain_Mapping__c' : '',
		#		'Support_Package__c': 'Standard', 
		#		'type': 'Account', 
		#		'Id': '0013000001AAZTKAA5', 
		#		'Account_Owner_Name__c': 'John Novak'
		#	}
		try:
			tam = "tam:" + record['Technical_Account_Manager__r']['Name'].replace(" ", "_").strip()
		except:
			tam = ''

		if record['Account_Owner_Name__c'] != '':
			ao = "ao:" + record['Account_Owner_Name__c'].replace(" ", "_").strip()
		else:
			ao = ''

		if record['Named_Support_Engineer__c'] != '':
			nse = "nse:" + record['Named_Support_Engineer__c'].replace(" ", "_").strip()
		else:
			nse = ''

		if record['Zendesk__Domain_Mapping__c'] != '':
			domains = string.split(record['Zendesk__Domain_Mapping__c'])
		else:
			domains = []
			
		if record['Twitter_Rate_Approval__c'] != '':
			twitter = "twitter_rate_approval:" + record['Twitter_Rate_Approval__c'].replace(" ", "_").strip()
		else:
			twitter = ''

		processedResults.append(
			{
				'Name'					: record['Name'],
				'NSE' 					: nse,
				'TAM' 					: tam,
				'Subscription' 			: record['Subscription_Plan__c'].replace(" ", ""),
				'SupportPackage' 		: record['Support_Package__c'],
				'AccountId' 			: record['Id'],
				'DomainMapping' 		: domains,
				'AccountOwner' 			: ao,
				'TwitterRateApproval' 	: twitter
			}
		)
	return processedResults

def UpsertZendeskOrgs(zendesk_conn, zendeskOrgs, sfdcAccounts):
	# 
	# {
  	#	u'name': u'WebTrends',
	#	  u'shared_comments': False,
	#	  u'url': u'https://datasiftsupport.zendesk.com/api/v2/organizations/23820183.json',
	#	  u'created_at': u'2013-02-08T23:35:14Z',
	#	  u'tags': [
	#	    
	#	  ],
	#	  u'updated_at': u'2013-03-23T15:53:36Z',
	#	  u'domain_names': [
	#	    
	#	  ],
	#	  u'details': None,
	#	  u'notes': None,
	#	  u'group_id': None,
	#	  u'external_id': None,
	#	  u'id': 23820183,
	#	  u'shared_tickets': False
	#	}

	# Support Packages
	elite = config.zenElite
	eliteVip = config.zenVip
	premier = config.zenPremier
	standard = config.zenStandard

	zendeskName = {} # Name-based key
	zendeskExtId = {} # SFDC-based key
	zendeskString = {} # Zendesk-ID based key to store string
	apiCalls = 0
	errorCount = 0

	for org in zendeskOrgs:
		if org['external_id'] is not None:
			zendeskExtId[org['external_id']] = org['id']
			zendeskString[org['id']] = "{0}_{1}_{2}_{3}_{4}".format(org['name'].encode('utf-8'), org['external_id'], org['group_id'], "_".join(sorted(org['domain_names'])), "_".join(sorted(org['tags'])))
		else:
			zendeskName[org['name']] = org['id']

	for account in sfdcAccounts:
		# Find the appropriate Support Group by the Package
		if account['SupportPackage'] == 'Elite':
			group = elite
		elif account['SupportPackage'] == 'Elite VIP':
			group = eliteVip
		elif account['SupportPackage'] == 'Premier':
			group = premier
		else:
			group = standard
		### FIRST, Try ID-based matching ###
		if account['AccountId'] in zendeskExtId:
			tags = [ 	
						account['AccountOwner'].lower(), 
						account['TAM'].lower(),
						account['NSE'].lower(),
						account['Subscription'].lower(),
						account['TwitterRateApproval'].lower()
					]
			tags = [x for x in tags if x] # remove empty strings
			
			sfdc = "{0}_{1}_{2}_{3}_{4}".format(account['Name'], account['AccountId'], group, "_".join(sorted(account['DomainMapping'])), "_".join(sorted(tags)))
			data = {
				"organization": {
						'name' : account['Name'].decode('utf-8'),
						'shared_comments' : True,
						'shared_tickets' : True,
						'domain_names' : account['DomainMapping'],
						'external_id' : account['AccountId'],
						'group_id' : group,
						'tags' : [
									account['AccountOwner'], 
									account['TAM'],
									account['NSE'],
									account['Subscription'],
									account['TwitterRateApproval']
								]
						}
			}
			
			try:
				#print "Before: " + str(data) # Before
				if sfdc != zendeskString[zendeskExtId[account['AccountId']]].encode('utf-8'):
					print "SFDC: " + sfdc
					print "ZenD: " + zendeskString[zendeskExtId[account['AccountId']]]
					orgId = zendeskExtId[account['AccountId']]
					result = zendesk_conn.update_organization(orgId, data)
					apiCalls+=1
					print "After: " + str(result) # After
					logging.debug("Zendesk Update: " + str(result))
			except Exception, err:
				print "Zendesk Update Organization Error for ID {0} : {1}".format(zendeskExtId[account['AccountId']], err)
				print err
				print data['organization']['name']
				logging.exception("Zendesk Update Organization Error for ID {0} : {1}".format(zendeskExtId[account['AccountId']], err))
				logging.warning("Data: {0}".format(data))
				errorCount+=1
		### SECOND - Try to match by organization name ###
		elif account['Name'] in zendeskName:
			data = {
				"organization": {
						'name' : account['Name'].decode('utf-8'),
						'shared_comments' : True,
						'shared_tickets' : True,
						'domain_names' : account['DomainMapping'],
						'external_id' : account['AccountId'],
						'group_id' : group,
						'tags' : [
									account['AccountOwner'], 
									account['TAM'],
									account['NSE'],
									account['Subscription'],
									account['TwitterRateApproval']
								]
						}
			}
			try:
				print "Before: " + str(data) # Before
				orgId = zendeskName[account['Name']]
				result = zendesk_conn.update_organization(orgId, data)
				apiCalls+=1
				print "After: " + str(result) # After
			except Exception, err:
				logging.exception("Zendesk Update Organization Error for Name {0} : {1}".format(zendeskName[account['Name']], err))
				logging.warning("Data: {0}".format(data))
				errorCount+=1
		### THIRD - Try to create the organization in Zendesk ###
		else:
			#print "Need to create org: " + str(account['Name'])
			data = {
				"organization": {
						'name' : account['Name'].decode('utf-8'),
						'shared_comments' : True,
						'shared_tickets' : True,
						'domain_names' : account['DomainMapping'],
						'external_id' : account['AccountId'],
						'group_id' : group,
						'tags' : [
									account['AccountOwner'], 
									account['TAM'],
									account['NSE'],
									account['Subscription'],
									account['TwitterRateApproval']
								]
						}
			}
			try:
				result = zendesk_conn.create_organization(data)
				apiCalls+=1
			except Exception, err:
				logging.exception("Zendesk Create Organization Error: {0} : {1}".format(account['Name'], err))
				logging.warning("Data: {0}".format(data))
				
				msg = json.loads(err.msg)
				
				if "description" in msg["details"]["name"][0]:
					print "exists"
				else:
					print "does not exist"

				errorCount += 1

	logging.info("Total Zendesk Update calls used: {0}".format(apiCalls))

	return errorCount

if __name__ == "__main__":
	# Create object for internal database methods (mySQL)
	mysqlDb = MySqlTask(config.mysql_username, config.mysql_password, config.mysql_host, config.mysql_database)

	# Create object for Salesforce methods
	sfdc = SalesforceTask(config.sfUser, config.sfPass, config.sfApiToken)

	# Create object for Zendesk methods
	zd = ZendeskTask(config.zenURL, config.zenAgent, config.zenPass, config.zenToken)

	### Pull SFDC Account Info to Zendesk Organizations ###

	try:
		sfdcLastModified = sfdc.sfdc_timestamp()
		logging.info("Current Salesforce Timestamp: {0}".format(sfdcLastModified))
	except Exception, err:
		logging.exception(err)
		sys.exit()

	# Get the last modified timestamp from internal database
	startTime = mysqlDb.pull_job_timestamp('SFDC_ACCOUNTS_LAST_MODIFIED')
	logging.info("Pulling SFDC_ACCOUNTS_LAST_MODIFIED; start time of {0}".format(startTime))

	##### Extract all SFDC Accounts #####
	logging.info("Pulling Accounts from Salesforce")
	sfdcQuery = """SELECT Id, Name, Subscription_Plan__c, Support_Package__c, Zendesk__Domain_Mapping__c, Account_Owner_Name__c, Named_Support_Engineer__c, Technical_Account_Manager__r.Name, Twitter_Rate_Approval__c  
				FROM Account WHERE LastModifiedDate > {0}""".format(startTime)
	sfdcResults = sfdc.sfdc_query(sfdcQuery)
	sfdcAccounts = ProcessSfdcAccounts(sfdcResults['results'])

	# If there are no SFDC Account modified since the last run, update the internal 
	# timestamp and exit.
	if len(sfdcAccounts) == 0:
		mysqlDb.update_job_timestamp('SFDC_ACCOUNTS_LAST_MODIFIED', sfdcLastModified)
		logging.info("Updating SFDC_ACCOUNTS_LAST_MODIFIED timestamp.")
		logging.info("No modified SFDC Accounts. Exiting.")
		sys.exit()

	else:
		logging.info("SFDC Account(s) Pulled: {0}".format(len(sfdcAccounts)))


	##### Extract all Zendesk Organizations #####
	try:
		zendeskOrgs = zd.get_all_organizations()

	except Exception, err:
		logging.exception(err)

	##### Update Zendesk Orgs #####
	errCount = UpsertZendeskOrgs(zd, zendeskOrgs, sfdcAccounts)

	##### Update Zendesk Organization Custom Fields #####
	org_fields = zd.list_organization_fields()

	if errCount:
		logging.info("Completed syncing Zendesk Organizations and SFDC Accounts. Errors were encountered and timestamp not updated.")
	else:
		mysqlDb.update_job_timestamp('SFDC_ACCOUNTS_LAST_MODIFIED', sfdcLastModified)
		logging.info("Completed syncing Zendesk Organizations and SFDC Accounts.")

