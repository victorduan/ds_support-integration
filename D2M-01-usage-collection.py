#!/usr/bin/python

import os
import sys
import logging
import config
import ConfigParser
import datasift
import time
import calendar
from datetime import datetime
from env import MySqlTask

if config.logLevel == "info":
	logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.INFO, filename=config.logFile)
else:
	logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.DEBUG, filename=config.logFile)


if __name__ == "__main__":
	_table_name = 'usage_reporting'

	# Create object for internal database methods (mySQL)
	mysql = MySqlTask(config.mysql_username, config.mysql_password, config.mysql_host, config.mysql_database)

	retry = 3
	while retry:
		try:
			logging.info("Getting valid columns from MySQL.")
			valid_sources = mysql.return_columns(_table_name)
			valid_sources = [ col for col in valid_sources if col not in ('intID', 'username', 'start', 'end', 'stream_type', 'stream_hash', 'seconds') ]
			logging.debug("Columns found: {0}".format(valid_sources))
			retry = 0
		except Exception, err:
			#print err #2013: Lost connection to MySQL server during query
			logging.error(err)
			retry -= 1
			logging.warning("Retries left: {0}".format(retry))
			time.sleep(2) # Sleep 2 seconds before retrying
	
	user_list = ConfigParser.ConfigParser()
	user_list.optionxform = str
	
	path = os.path.dirname(os.path.realpath(__file__))
	user_list.read(path+"/user_list.ini")
	
	print "Users to query /usage: {0}".format(user_list.options("Users"))

	for username in user_list.options("Users"):
		api_key = user_list.get("Users", username)
		
		# Connect to the mysql database
		mysql.connect()

		# Create object for DataSift methods
		ds = datasift.User(username, api_key)

		retry = 3
		while retry:
			try:
				usage = ds.get_usage('day')
				logging.info("Getting /usage for user: {0}".format(username))
				print "Getting /usage for user: {0}".format(username)
				retry = 0
			except Exception, err:
				logging.error("Encountered getting /usage for user: {0}. Error message: {1}".format(username, err))
				retry -= 1
				logging.warning("Retries left: {0}".format(retry))
				time.sleep(5) # Sleep 5 seconds before retrying


		date_format = "%a, %d %b %Y %H:%M:%S +0000"
		start 		= time.strptime(usage['start'], date_format)
		end 		= time.strptime(usage['end'], date_format)

		unix_start 	= calendar.timegm(start)
		unix_end	= calendar.timegm(end)

		insert_string = ''
		
		if len(usage['streams']):
		
			for stream in usage['streams']:
				if len(stream) == 32:
					stream_type = "stream"
				else:
					stream_type = "historic"
	
				seconds = usage['streams'][stream]['seconds']
	
				data = {
					'username'		: username,
					'start'			: unix_start,
					'startDate'		: datetime.utcfromtimestamp(unix_start),
					'end'			: unix_end,
					'endDate'		: datetime.utcfromtimestamp(unix_end),
					'stream_type'	: stream_type,
					'stream_hash' 	: str(stream),
					'seconds'		: seconds
				}
	
				licenses = usage['streams'][stream]['licenses']
	
				if len(licenses):
					headers = []
					for license_type, license_value in licenses.items():
						# Only add licenses for columns that exist in the database
						if any(str(license_type) in x for x in valid_sources):
							data[str(license_type)] = license_value
							headers.append(str(license_type))
	
					fields_string = ", ".join([ "`{0}`".format(k) for k in headers ])
					values_string = ", ".join([ "%({0})s".format(k) for k in headers ])
	
					insert_query = ("""
									INSERT INTO {0} 
									(`username`, `start`, `startDate`, `end`, `endDate`, `stream_type`, `stream_hash`, `seconds`, {1}) 
									VALUES ('%(username)s', %(start)s, '%(startDate)s', %(end)s, '%(endDate)s', '%(stream_type)s', '%(stream_hash)s', %(seconds)s, {2});
									""").format(_table_name, fields_string, values_string)
	
				# Different MySQL Query if there is no license consumption
				else:
					insert_query = ("""
									INSERT INTO {0} 
									(`username`, `start`, `startDate`, `end`, `endDate`, `stream_type`, `stream_hash`, `seconds`) 
									VALUES ('%(username)s', %(start)s, '%(startDate)s', %(end)s, '%(endDate)s', '%(stream_type)s', '%(stream_hash)s', %(seconds)s);
									""").format(_table_name)
									
				# Concatenate all the INSERT statements    
				insert_string += " ".join(insert_query.split()) % data
	        	
			try:
				insert_count= 0
				cursor = mysql.execute_many(insert_string)
				for insert in cursor:
					insert_count += 1
					
				# Commit the inserts for the user (if there are results)
				if insert_count: mysql.commit()
				else:			mysql.close()
			except Exception, err:
				logging.exception(err)
				logging.error("Query: {0}".format(insert_string))
				continue
				
		else:
			logging.info("No streams consumed in the past 24 hours for user: {0}".format(username)) 
		
	logging.info("Tasks completed.")

