#!/usr/bin/python
"""
This contains classes for commonly-used
operations. Requires the Beatbox 20.0, 
MySQL Connector and Zendesk Python packages

"""
__author__ = "Victor Duan <victor@datasift.com>"
__version__ = "0.1.0"
__date__ = "04/09/2013"

# Custom Modules
import beatbox
import mysql.connector
from mysql.connector.constants import ClientFlag
from zendesk import Zendesk

# Other Imports
import datetime
import calendar
import time
import json
import urllib
import requests

class SalesforceTask(object):

	_username = ""
	_password = ""
	_api_token = ""

	# SFDC Variables
	sf = beatbox._tPartnerNS
	svc = beatbox.PythonClient()
	beatbox.gzipRequest=False

	def __init__(self, username, password, api_token=''):
		self._username = username
		self._password = password
		self._api_token = api_token

	def sfdc_connect(self):
		return self.svc.login(self._username, self._password + self._api_token)

	def sfdc_timestamp(self):
		self.sfdc_connect()
		return self.svc.getServerTimestamp()

	def sfdc_query(self, query):
		self.sfdc_connect()

		results = []

		qr = self.svc.query(query)

		# Check to see if any work needs to be done by checking result size
		if qr['size']:
			queryComplete = 'false'
		else:
			queryComplete = 'true'

		# Process results and query more, if necessary
		while queryComplete == 'false':
			for record in qr:
				results.append(record)
				
			if not qr['done']:
				qr = self.svc.queryMore(qr['queryLocator'])
			else:
				queryComplete = 'true'

		return { "count" : qr['size'], "results" : results }

	def update_sfdc_object(self, objectName, fieldName, data, batchSize=200):
		batchedData = [] # List to upload in batches to SFDC
		apiCalls = 0 # Count of API calls used by the function
		numBatches = 0 # Number of batches needed

		numBatches = len(data)/batchSize
		if (len(data)%batchSize): # If there is a remainder, add 1
			numBatches += 1

		for sfdcId in data:
			batchedData.append(
					{
						'type' : objectName,
						'Id' : sfdcId,
						fieldName : data[sfdcId]
					}
				)

			if len(batchedData) == batchSize:
				apiCalls += 1
				#print "Upserting batch of " + str(batchSize) + " into " + objectName + " ... proceeding with batch " + str(apiCalls) + " of " + str(numBatches)
				print "Upserting batch of {0} into {1} ... proceeding with batch {2} of {3}".format(batchSize, objectName, apiCalls, numBatches)
				ur = self.svc.upsert('Id', batchedData)
				batchedData = []

		# Upsert any remaining batch
		if len(batchedData) > 0:
			apiCalls += 1
			print "Upserting batch of {0} into {1} ... proceeding with batch {2} of {3}".format(batchSize, objectName, apiCalls, numBatches)
			ur = self.svc.upsert('Id', batchedData)
			batchedData = []

		print "Total API calls used updating {0}: {1}".format(objectName, apiCalls)

class MySqlTask(object):

	_username 	= ""
	_password 	= ""
	_host 		= ""
	_database 	= ""

	def __init__(self, username, password, host, database):
		self._username = username
		self._password = password
		self._host = host
		self._database = database

	def pull_job_timestamp(self, job_name):
		# Open MySQL Connection
		cnx = mysql.connector.connect(user=self._username,  password=self._password, host=self._host, database=self._database)

		# Find the last time from the database - to use as a start time for SFDC pull
		timeCursor = cnx.cursor()
		timeQuery = ("SELECT jobvalue FROM job_timestamps WHERE jobname = %s")
		timeCursor.execute(timeQuery, (job_name, ))
		for (jobvalue) in timeCursor:
			start_time = jobvalue

		cnx.commit()
		timeCursor.close()
		cnx.close()

		return start_time[0]
	
	def get_timezone_by_zip(self, zipcode):
		timezone = ""
		
		query = "SELECT * FROM timezones_by_zip WHERE zip = {0}".format(zipcode)
		
		# Open MySQL Connection
		self.connect()
		
		cursor = self.execute_query(query)
		
		for row in cursor:
			timezone = dict(zip(cursor.column_names, row))['timezone']
		
		self.close()
		
		return timezone

	def update_job_timestamp(self, job_name, timestamp):
		# Open MySQL Connection
		cnx = mysql.connector.connect(user=self._username,  password=self._password, host=self._host, database=self._database)

		# Find the last time from the database - to use as a start time for SFDC pull
		timeCursor = cnx.cursor()
		storeTime = ("UPDATE job_timestamps SET last_run=now(), jobvalue=%s WHERE jobname=%s")
		timeCursor.execute(storeTime, (timestamp, job_name))

		cnx.commit()
		timeCursor.close()
		cnx.close()

	def return_columns(self, table_name):
		# Open MySQL Connection
		cnx = mysql.connector.connect(user=self._username,  password=self._password, host=self._host, database=self._database)
		columns = []
		query = "show columns from {0}".format(table_name)
		cursor = cnx.cursor()
		cursor.execute(query)

		for (Field, Type, Null, Key, Default, Extra) in cursor:
			columns.append(Field)

		cnx.commit()
		cursor.close()
		cnx.close()

		return columns

	def connect(self):
		flags = [ClientFlag.MULTI_STATEMENTS]
		self._cnx = mysql.connector.connect(user=self._username,  password=self._password, host=self._host, database=self._database, client_flags=flags)
		return self._cnx

	def select_query(self, query):
		# Executes a query against the current database and returns the results
		cursor = self._cnx.cursor()
		cursor.execute(query)
		results	= []
		for row in cursor:
			results.append(row)

		self._cnx.commit()
		cursor.close()

		return results

	def execute_query(self, query, data=''):
		self._cnx.autocommit = False
		
		self._cursor = self._cnx.cursor()
		self._cursor.execute(query, data)
		
		return self._cursor
	
	def execute_many(self, query):
		self._cnx.autocommit = False
		self._cursor = self._cnx.cursor()

		return self._cursor.execute(query, multi=True)

	def commit(self):
		self._cnx.commit()
		self._cursor.close()
		self._cnx.close()   
		
	def close(self):
		self._cnx.close() 

class ZendeskTask(object):

	_username 	= ""
	_url 		= ""
	_password 	= ""
	_token 		= ""
	_headers	= {}

	def __init__(self, url, username, password, token):
		self._username = username
		self._password = password
		self._url = url
		self._token = token
		self._zd = Zendesk(self._url, self._username, self._password, self._token, api_version=2)
		self._headers = {'content-type': 'application/json'}

	def search_by_email(self, email):
		return self._zd.search_user(query=email)

	def get_all_organizations(self):
		zen_orgs = []
		runLoop = True
		page = 1

		while runLoop:		
			results = self._zd.list_organizations(page=page)

			if int(results['count']) > 0:
				for org in results['organizations']:
					zen_orgs.append(org)
				page+=1
			if results['next_page'] is None:
				runLoop = False

		return zen_orgs

	def update_organization(self, orgId, data):
		return self._zd.update_organization(organization_id=orgId, data=data)

	def update_organization_v2(self, orgId, data):
		# Use requests module instead
		username = self._username + '/token'
		url = '{0}/api/v2/organizations/{1}.json'.format(self._url, orgId)
		r = requests.put(url, auth=(username, self._password), headers=self._headers, data=json.dumps(data))
		return r.content

	def create_organization(self, data):
		return self._zd.create_organization(data=data)

	def create_organization_v2(self, data):
		# Use requests module instead
		username = self._username + '/token'
		url = '{0}/api/v2/organizations.json'.format(self._url)
		r = requests.post(url, auth=(username, self._password), headers=self._headers, data=json.dumps(data))
		return r.content
	
	def update_user(self, userId, data):
		return self._zd.update_user(user_id=userId, data=data)

	def list_organization_fields(self):
		r = self._zd.list_organization_fields()
		return { "count" : r['count'], "fields" : r['organization_fields'] }

	def search_organization_by_name(self, name):
		return self._zd.autocomplete_organizations(name=name)

	def get_existing_organization_field_options(self, field_id):
		return self._zd.show_organization_field(field_id=field_id)['organization_field']['custom_field_options']

	def add_organization_field_dropdown(self, field_id, field_name='Vict0r Duan', field_value='test1'):
		# Dropdown fields need all options to be added
		existing_fields = self.get_existing_organization_field_options(field_id=field_id)

		existing_fields.append({ 'name' : field_name, 'value' : field_value })
		data = { "organization_field" : { "custom_field_options" : [{"name": "test", "value": "test2"}] }}
		try:
			print data
			print self._zd.update_organization_fields(field_id=field_id, data=data)
		except Exception, err:
			print err
			return self.get_existing_organization_field_options(field_id=field_id)

		return self.get_existing_organization_field_options(field_id=field_id)
	
	def get_tickets(self, start_time):
		tickets 		= []
		runLoop 		= True
		current_time	= datetime.datetime.now() - datetime.timedelta(minutes=30)
		current_unix	= calendar.timegm(current_time.utctimetuple())
		
		while runLoop:
			try:
				results = self._zd.export_incremental_tickets(start_time=start_time)
				
			except Exception, err:
				if err.error_code == 429:
					# Handles message "Number of allowed incremental ticket export API requests per minute exceeded"
					time.sleep(int(err.retry_after) + 1)
					continue
				
			start_time = results['end_time']
			
			for ticket in results['results']:
				tickets.append(ticket)
			
			# Sleeping an arbitrary 10 seconds
			time.sleep(10)
			
			if start_time > current_unix:
				# Stops the loop if the time is within 6 minutes of the current time
				runLoop = False
				
		return { 'end_time' : start_time, 'results' : tickets }
