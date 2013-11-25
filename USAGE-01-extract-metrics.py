from env import MySqlTask
from env import SalesforceTask
import config
import reportingconfig

import datetime
from datetime import date
import calendar
import time
import os
import csv
import sys
import locale

def usage(message = '', exit = True):
	"""
	Display usage information, with an error message if provided.
	"""
	if len(message) > 0:
		sys.stderr.write('\n%s\n' % message)
		sys.stderr.write('\n');
		sys.stderr.write('Usage: USAGE-01-extract-metrics.py <month number> <4-digit year> \\\n')
		sys.stderr.write('\n')
		sys.stderr.write('Example\n')
		sys.stderr.write('       USAGE-01-extract-metrics.py 8 2013 \\\n')
		sys.stderr.write('\n')
		sys.stderr.write('\n')
	if exit:
		sys.exit(1)

def QueryMySql(query):
	# Create object for internal database methods (mySQL)
	mysqlDb = MySqlTask(config.mysql_username, config.mysql_password, config.mysql_host, config.mysql_database)
	mysqlDb.connect()

	results = mysqlDb.select_query(query)

	mysqlDb.close()

	return results

def MonthlyDPU(month_num, year, user_name, path):
	fromDate 	= datetime.date(year=year, month=month_num, day=1)
	query = """
			SELECT sum(dpus) FROM dpu_usage
			WHERE date >= '{0}' and date < date_add('{0}', interval 1 month) and user_id in (SELECT user_id FROM users WHERE username = '{1}');
			""".format(fromDate, user_name)

	results = QueryMySql(query)

	for row in results:
		dpu = locale.format("%d", row[0], grouping=True)

	f = open(path+"/dpu.txt", 'w')
	f.write(dpu)
	f.close()

def DailyDpuBreakdown (month_num, year, user_name, path):
	fromDate 	= datetime.date(year=year, month=month_num, day=1)
	dateString	= "%m/%d/%y"
	csvHeader	= { 'date', 'DPUs' }
	csvData		= []
	query = """
			SELECT calendar.date 'Date', dpu_usage.dpus `DPUs` from `calendar` 
			LEFT JOIN `dpu_usage`
			ON calendar.date = dpu_usage.date AND dpu_usage.user_id in (SELECT user_id FROM users WHERE username = '{0}')
			WHERE calendar.date >= '{1}' and calendar.date < date_add('{1}', interval 1 month);
			""".format(user_name, fromDate)

	print query

	results = QueryMySql(query)
	
	for row in results:
		if row[1] is None: dpus = '0'
		else: dpus = row[1]
		print "%s : %s" % (row[0].strftime(dateString), dpus)
		csvData.append({ 'date' : row[0].strftime(dateString), 'DPUs' : dpus })

	WriteCsv(csvHeader, path, 'daily_dpu_breakdown.csv', csvData)

def MonthOverMonth(month_num, year, user_name, path):
	outerDate 	= datetime.date(year=year, month=month_num, day=1)
	csvHeader	= { 'date', 'DPUs' }
	csvData		= []
	query = """
			SELECT month(calendar.date), year(calendar.date), sum(dpu_usage.dpus) from calendar 
			LEFT JOIN dpu_usage
			ON calendar.date = dpu_usage.date AND dpu_usage.user_id in (SELECT user_id FROM users WHERE username = '{0}')
			WHERE calendar.date > date_sub('{1}', interval 6 month) and calendar.date < date_add('{1}', interval 1 month) 
			GROUP BY month(calendar.date);
			""".format(user_name, outerDate)

	print query

	results = QueryMySql(query)

	for row in results:
		if row[2] is None : dpus = '0'
		else : dpus = row[2]
		date = str(row[0]) + '/1/' + str(row[1])

		print "%s : %s" % (date, dpus)
		csvData.append({ 'date' : date, 'DPUs' : dpus })

	WriteCsv(csvHeader, path, 'month_over_month.csv', csvData)

def DataSourceVsAugmentations(month_num, year, user_name, path):
	fromDate	= datetime.date(year=year, month=month_num, day=1)
	endDate		= fromDate + datetime.timedelta(days=calendar.monthrange(year,month)[1])
	dateString	= "%m/%d/%y"
	csvHeader	= { 'date', 'Data Source', 'Augmentation' }
	csvData		= []
	pivotData 	= {}
	timeLoop 	= True
	query = """
			SELECT calendar.date, licenses.license_type, licenses.volume, licenses.fee 
			FROM calendar
			LEFT JOIN licenses
			ON calendar.date = licenses.date AND licenses.user_id in (SELECT user_id FROM users WHERE username = '{0}')
			WHERE calendar.date >= '{1}' and calendar.date < date_add('{1}', interval 1 month);
			""".format(user_name, fromDate)
	
	print query
	
	results = QueryMySql(query)

	# Get days in the month to be generated as keys for dictionary
	while timeLoop:
		date = fromDate.strftime(dateString)
		pivotData[date] = { 'Data Source' : 0, 'Augmentation' : 0 }
		fromDate += datetime.timedelta(days=1)
		if fromDate == endDate : 
			timeLoop = False

	# Pivot the SQL results to suit the format for D3
	for row in results:
		date = row[0].strftime(dateString)

		if row[1] is None : 
			pivotData[date]['Augmentation'] += 0
		else :
			try : 
				license_type = reportingconfig.dataSourceMapping[row[1]]['type']
				pivotData[date][license_type] += int(row[2])
			except:
				# Create a new dictionary key
				pivotData[date][row[1]] = int(row[2])

	# Generate the CSV file
	for date, data in sorted(pivotData.items()):
		csvData.append({ 'date' : date, 'Data Source' : data['Data Source'], 'Augmentation' : data['Augmentation'] })

	WriteCsv(csvHeader, path, 'data_source_vs_augmentation.csv', csvData)

def LicenseBreakdownPie(month_num, year, user_name, path):
	fromDate	= datetime.date(year=year, month=month_num, day=1)
	csvDataVolume	= []
	csvDataFees		= []
	csvHeader 		= { 'name', 'value' }
	query = """
			SELECT license_type, sum(volume), sum(fee) 
			FROM licenses
			WHERE user_id in (SELECT user_id FROM users WHERE username = '{0}') and date >= '{1}' and date < date_add('{1}', interval 1 month)
			GROUP BY license_type;
			""".format(user_name, fromDate)

	results = QueryMySql(query)

	print query

	for row in results:
		rowName = reportingconfig.dataSourceMapping[row[0]]['label']
		csvDataVolume.append({ 'name' : rowName, 'value' : row[1] })
		csvDataFees.append({ 'name' : rowName, 'value' : row[2] })

	WriteCsv(csvHeader, path, 'license_volume_pie.csv', csvDataVolume)
	WriteCsv(csvHeader, path, 'license_fees_pie.csv', csvDataFees)

def GetUsers():
	users   	= []

	query = """
			SELECT Id, Username_s__c 
			FROM Account 
			WHERE Support_Package__c in ('Premier', 'Elite', 'Elite VIP') and Account_Status__c = 'Customer'
			"""

	# Create object for Salesforce methods
	sfdc = SalesforceTask(config.sfUser, config.sfPass, config.sfApiToken)
	results = sfdc.sfdc_query(query)

	for row in results['results']:
		user_list = row['Username_s__c'].split(',')
		for user in user_list:
			users.append(user)

	return users


def WriteCsv (headers, path, filename, data):
	with open(path + '/' + filename, 'wb') as csvfile:
		csvwriter = csv.DictWriter(csvfile, delimiter=',', fieldnames=headers, extrasaction='ignore')
		csvwriter.writerow(dict(zip(csvwriter.fieldnames, csvwriter.fieldnames)))
		for row in data:
			csvwriter.writerow(row)

if __name__ == "__main__":
	locale.setlocale(locale.LC_ALL, config.locale)
	if len(sys.argv) < 2:
		sys.stderr.write('Please specify the month and year as the two command line arguments!\n')
		usage()

	month 	= int(sys.argv[1])
	year	= int(sys.argv[2])
	runOnce	= False

	if month > 12 or month < 1:
		sys.stderr.write('Please check your month value!\n')
		usage()

	if year > date.today().year + 1:
		sys.stderr.write('Please check your year value! Too far into the future!\n')
		usage()

	if len(sys.argv[3:]):
		username 	= sys.argv[3]
		runOnce		= True

	# Directory Structure :: /path/to/script/year/month/username
	workingDir = reportingconfig.dataDirectory + "/" + str(year) + "/" + str(month)
	print workingDir

	if not os.path.exists(workingDir):
		os.makedirs(workingDir)

	if not runOnce:
		# Select all users in the month
		users = GetUsers()
		sys.exit()
		if len(users):
			for user in users:
				userPath = workingDir + '/' + user
				if not os.path.exists(userPath) : os.makedirs(userPath)
				MonthlyDPU(month, year, user, userPath)
				DailyDpuBreakdown(month, year, user, userPath)
				MonthOverMonth(month, year, user, userPath)
				DataSourceVsAugmentations(month, year, user, userPath)
				LicenseBreakdownPie(month, year, user, userPath)

	else:
		user = username
		userPath = workingDir + '/' + user
		if not os.path.exists(userPath) : os.makedirs(userPath)
		MonthlyDPU(month, year, user, userPath)
		DailyDpuBreakdown(month, year, user, userPath)
		MonthOverMonth(month, year, user, userPath)
		DataSourceVsAugmentations(month, year, user, userPath)
		LicenseBreakdownPie(month, year, user, userPath)