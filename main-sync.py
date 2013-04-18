#!/usr/bin/python

import sys
import logging
import config
import json
import string
import beatbox
import time
import mysql.connector
from zendesk import Zendesk
from helperclass import MySqlTask

if config.logLevel == "info":
	logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.INFO, filename=config.logFile)
else:
	logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.DEBUG, filename=config.logFile)

def QuerySfdc(query):
	# SFDC Variables
	sf = beatbox._tPartnerNS
	svc = beatbox.PythonClient()
	beatbox.gzipRequest=False
	sf_username = config.sfUser
	sf_password = config.sfPass
	sf_api_token = config.sfApiToken

	try:
		logging.info("Logging into Salesforce")
		login = svc.login(sf_username, sf_password + sf_api_token)

	except Exception, err:
		print err
		logging.error("Problem logging into Salesforce: " + str(err))
		logging.error("Exiting...")
		sys.exit()

	print "Login successful ("+ str(login) +")." #DEBUG
	logging.debug("Login successful ("+ str(login) +").")

	results = []

	logging.debug("Running Salesforce Query: " + str(query))
	qr = svc.query(query)

	print "Result size: " + str(qr['size'])
	logging.info("Salesforce Query Result Count: " + str(qr['size']))

	# Check to see if any work needs to be done by checking result size
	if qr['size']:
		queryComplete = 'false'
	else:
		queryComplete = 'true'
		print "Nothing to do."
		logging.info("No Salesforce data, moving on")

	# Process results and query more, if necessary
	while queryComplete == 'false':
		for record in qr:
			results.append(record)
			
		if not qr['done']:
			qr = svc.queryMore(qr['queryLocator'])
		else:
			queryComplete = 'true'

	return results

def PullSfdcAccounts(lastModifiedDate='2012-01-01T00:00:00.000Z'):

	query = "SELECT Id, Name, Subscription_Plan__c, Support_Package__c, Zendesk__Domain_Mapping__c, Account_Owner_Name__c, Named_Support_Engineer__c, Technical_Account_Manager__r.Name FROM Account WHERE LastModifiedDate > " + lastModifiedDate

	processedResults = []

	results = QuerySfdc(query)
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

		processedResults.append(
			{
				'Name': record['Name'],
				'NSE' : nse,
				'TAM' : tam,
				'Subscription' : record['Subscription_Plan__c'],
				'SupportPackage' : record['Support_Package__c'],
				'AccountId' : record['Id'],
				'DomainMapping' : domains,
				'AccountOwner' : ao
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
			zendeskString[org['id']] = org['name'] + "_" + org['external_id'] + "_" + str(org['group_id']) + "_" + "_".join(sorted(org['domain_names'])) + "_" + "_".join(sorted(org['tags']))
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
						account['Subscription'].lower()
					]
			tags = [x for x in tags if x] # remove empty strings
			
			sfdc = account['Name'] + "_" + account['AccountId'] + "_" + str(group) + "_" + "_".join(sorted(account['DomainMapping'])) + "_" + "_".join(sorted(tags))
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
									account['Subscription']
								]
						}
			}
			
			try:
				#print "Before: " + str(data) # Before
				if sfdc != zendeskString[zendeskExtId[account['AccountId']]].encode('utf-8'):
					print "SFDC: " + sfdc
					print "ZenD: " + zendeskString[zendeskExtId[account['AccountId']]]
					result = zendesk_conn.update_organization(organization_id=zendeskExtId[account['AccountId']], data=data)
					apiCalls+=1
					print "After: " + str(result) # After
					logging.debug("Zendesk Update: " + str(result))
			except Exception, err:
				print "Zendesk Update Organization Error for ID " + str(zendeskExtId[account['AccountId']]) + " : " +  str(err)
				print err
				print data['organization']['name']
				logging.warning("Zendesk Update Organization Error for ID " + str(zendeskExtId[account['AccountId']]) + " : " +  str(err))
				logging.warning("Data: " + str(data))
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
									account['Subscription']
								]
						}
			}
			try:
				print "Before: " + str(data) # Before
				result = zendesk_conn.update_organization(organization_id=zendeskName[account['Name']], data=data)
				apiCalls+=1
				print "After: " + str(result) # After
			except Exception, err:
				print "Zendesk Update Organization Error for Name " + str(zendeskName[account['Name']]) + " : " +  str(err)
				print data['organization']['name']
				logging.warning("Zendesk Update Organization Error for Name " + str(zendeskName[account['Name']]) + " : " +  str(err))
				logging.warning("Data: " + str(data))
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
									account['Subscription']
								]
						}
			}
			try:
				#print "Before: " + str(data) # Before
				result = zendesk_conn.create_organization(data=data)
				apiCalls+=1
				#print "After: " + str(result) # After
			except Exception, err:
				print "Zendesk Create Organization Error: " + str(account['Name']) + " : " +  str(err)
				print "Data: " + str(data)
				logging.warning("Zendesk Create Organization Error: " + str(account['Name']) + " : " +  str(err))
				logging.warning("Data: " + str(data))
				errorCount+=1
		# Sleep 1 second between orgs
		#time.sleep(.25)

	print "Total Zendesk Update calls used: " + str(apiCalls)
	logging.info("Total Zendesk Update calls used: " + str(apiCalls))

	return errorCount

def PullZendeskOrgs(zendesk_conn):

	#### FIELDS #####
	# {
	#	"id"
	#	"external_id"
	#	"url"
	#	"name"
	#	"created_at"
	#	"updated_at"
	#	"domain_names"
	#	"details"
	#	"notes"
	#	"group_id"
	#	"tags"
	# }

	zenOrgs = []
	runLoop = True
	page = 1

	while runLoop:		
		results = zendesk_conn.list_organizations(page=page)
		if int(results['count']) > 0:
			for org in results['organizations']:
				#print org
				zenOrgs.append(org)
			page+=1
		if results['next_page'] is None:
			runLoop = False

	print "Total Zendesk Organization calls used: " + str(page)
	logging.info("Total Zendesk Organization calls used: " + str(page))

	return zenOrgs

if __name__ == "__main__":
	# Create object for internal database methods (mySQL)
	mysqlDb = MySqlTask(config.mysql_username, config.mysql_password, config.mysql_host, config.mysql_database)

	# Create object for Salesforce methods
	sfdc = SalesforceTask(config.sfUser, config.sfPass, config.sfApiToken)

	# Get the last modified timestamp from internal database
	startTime = mysqlDb.pull_job_timestamp('SFDC_ACCOUNTS_LAST_MODIFIED')
	print startTime
	logging.info("Using a start time of " + str(startTime))

	sfdcLastModified = sfdc.sfdc_timestamp()
	print "Current Salesforce Timestamp: " + str(sfdcLastModified)
	logging.info("Current Salesforce Timestamp: " + str(sfdcLastModified))

	##### Extract all SFDC Accounts #####
	logging.info("Pulling Accounts from Salesforce")
	sfdcAccounts = PullSfdcAccounts(startTime)

	# If there are no SFDC Account modified since the last run, update the internal 
	# timestamp and exit.
	if len(sfdcAccounts) == 0:
		mysqlDb.update_job_timestamp('SFDC_ACCOUNTS_LAST_MODIFIED', sfdcLastModified)
		logging.info("Updating SFDC_ACCOUNTS_LAST_MODIFIED timestamp.")
		logging.info("No modified SFDC Accounts. Exiting.")
		sys.exit()

	# Zendesk Variables
	zenURL = config.zenURL
	zenAgent = config.zenAgent
	zenPass = config.zenPass
	zenToken = config.zenToken

	logging.info("Connecting to Zendesk")
	zendesk = Zendesk(zenURL, zenAgent, zenPass, zenToken)

	##### Extract all Zendesk Organizations #####
	try:
		zendeskOrgs = PullZendeskOrgs(zendesk)

	except Exception, err:
		msg = json.loads(err.msg)
		print msg['error']['message']
		logging.error("Could not connect to Zendesk: " + msg['error']['message'])
		logging.error("Exiting...")
		sys.exit()

	##### Update Zendesk Orgs #####
	count = UpsertZendeskOrgs(zendesk, zendeskOrgs, sfdcAccounts)

	if count:
		logging.info("Completed syncing Zendesk Organizations and SFDC Accounts. Errors were encountered and timestamp not updated")
	else:
		mysqlDb.update_job_timestamp('SFDC_ACCOUNTS_LAST_MODIFIED', sfdcLastModified)
		logging.info("Completed syncing Zendesk Organizations and SFDC Accounts.")

