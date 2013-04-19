#!/usr/bin/python
"""
This contains mainly helper files to perform tasks. 
Requires the use of BeatBox 20.0 (<LINK>)

"""
__author__ = "Victor Duan <victor@datasift.com>"
__version__ = "0.1.0"
__date__ = "04/09/2013"

import beatbox
import mysql.connector

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
		self.sfdc_connect

		results = []

		qr = self.svc.query(query)

		print "Result size: " + str(qr['size'])

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

		return results

class MySqlTask(object):

	_username = ""
	_password = ""
	_host = ""
	_database = ""

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
		timeQuery = ("SELECT jobvalue FROM sfdc_jobs WHERE jobname = %s")
		timeCursor.execute(timeQuery, (job_name, ))
		for (jobvalue) in timeCursor:
			start_time = jobvalue

		cnx.commit()
		timeCursor.close()
		cnx.close()

		return start_time[0]

	def update_job_timestamp(self, job_name, timestamp):
		# Open MySQL Connection
		cnx = mysql.connector.connect(user=self._username,  password=self._password, host=self._host, database=self._database)

		# Find the last time from the database - to use as a start time for SFDC pull
		timeCursor = cnx.cursor()
		storeTime = ("UPDATE sfdc_jobs SET last_run=now(), jobvalue=%s WHERE jobname=%s")
		timeCursor.execute(storeTime, (timestamp, job_name))

		cnx.commit()
		timeCursor.close()
		cnx.close()

