#!/usr/bin/python

from datetime import datetime, timedelta
import time
import types
import logging
import config
import sys
from env import MySqlTask
from env import ZendeskTask

if config.logLevel == "info":
    logHandler = logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.INFO, filename=config.logFile)
else:
    logHandler = logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.DEBUG, filename=config.logFile)

def ConvertToUTC(timeString):
    #sampleTime = "2012-10-17 11:55:47 +0100"
    timeFormat = "%Y-%m-%d %H:%M:%S"

    if type(timeString) is types.NoneType:
        return ""

    offset = timeString[len(timeString)-5:]
    origDateTime = timeString[:len(timeString)-5].strip()
    timeToConvert = datetime.fromtimestamp(time.mktime(time.strptime(origDateTime, timeFormat)))

    #print timeToConvert

    if not offset.find("+"):
        minuteOffset = float(offset[1:]) * .6
        return str(timeToConvert - timedelta(minutes=minuteOffset))
    else:
        minuteOffset = float(offset[1:]) * .6
        return str(timeToConvert + timedelta(minutes=minuteOffset))

if __name__ == "__main__":
    logging.info("Running incremental Zendesk export...")

    # Create object for Zendesk queries
    zd = ZendeskTask(config.zenURL, config.zenAgent, config.zenPass, config.zenToken)
    
    # Create object for internal database methods (mySQL)
    mysqlDb = MySqlTask(config.mysql_username, config.mysql_password, config.mysql_host, config.mysql_database)

    # Grab the last time Accounts were successfully updated
    start_time = mysqlDb.pull_job_timestamp("INCREMENTAL_EXPORT")

    logging.info("INCREMENTAL_EXPORT: start_time={0}".format(start_time))
    try:
        tickets = zd.get_tickets(start_time)

    except Exception, err:
        logging.exception(err)
    #print tickets

    dbQuery = ("INSERT INTO zendesk_tickets "
                "VALUES (%(ticket_id)s, %(generated_timestamp)s, "
                    "%(req_name)s, %(req_id)s, %(req_external_id)s, "
                    "%(req_email)s, %(submitter_name)s, %(assignee_name)s, "
                    "%(group_name)s, %(subject)s, %(current_tags)s, "
                    "%(priority)s, %(via)s, %(ticket_type)s, %(created_at)s, "
                    "%(updated_at)s, %(assigned_at)s, %(organization_name)s, "
                    "%(due_date)s, %(initially_assigned_at)s, %(solved_at)s, "
                    "%(resolution_time)s, %(satisfaction_score)s, %(group_stations)s, "
                    "%(assignee_stations)s,%(reopens)s, %(replies)s, "
                    "%(first_reply_time_in_minutes)s, %(first_reply_time_in_minutes_within_business_hours)s, "
                    "%(first_resolution_time_in_minutes)s,%(first_resolution_time_in_minutes_within_business_hours)s,"
                    "%(full_resolution_time_in_minutes)s, %(full_resolution_time_in_minutes_within_business_hours)s, "
                    "%(agent_wait_time_in_minutes)s, %(agent_wait_time_in_minutes_within_business_hours)s, "
                    "%(requester_wait_time_in_minutes)s,%(requester_wait_time_in_minutes_within_business_hours)s, "
                    "%(on_hold_time_in_minutes)s, %(on_hold_time_in_minutes_within_business_hours)s, "
                    "%(url)s, %(datasift_username)s,%(category)s, %(status)s)"
                "ON DUPLICATE KEY UPDATE "
                    "generated_timestamp = %(generated_timestamp)s, "
                    "req_name = %(req_name)s, "
                    "req_id = %(req_id)s, "
                    "organization_name = %(organization_name)s, "
                    "req_external_id = %(req_external_id)s, "
                    "submitter_name = %(submitter_name)s, "
                    "assignee_name = %(assignee_name)s, "
                    "group_name = %(group_name)s, "
                    "subject = %(subject)s, "
                    "current_tags = %(current_tags)s, "
                    "priority = %(priority)s, "
                    "ticket_type = %(ticket_type)s, "
                    "updated_at = %(updated_at)s, "
                    "initially_assigned_at = %(initially_assigned_at)s, "
                    "assigned_at = %(assigned_at)s, "
                    "solved_at = %(solved_at)s, "
                    "resolution_time = %(resolution_time)s, "
                    "satisfaction_score = %(satisfaction_score)s, "
                    "due_date = %(due_date)s, "
                    "group_stations = %(group_stations)s, "
                    "assignee_stations = %(assignee_stations)s, "
                    "reopens = %(reopens)s, "
                    "replies = %(replies)s, "
                    "first_reply_time_in_minutes = %(first_reply_time_in_minutes)s, "
                    "first_reply_time_in_minutes_within_business_hours = %(first_reply_time_in_minutes_within_business_hours)s, "
                    "first_resolution_time_in_minutes = %(first_resolution_time_in_minutes)s, "
                    "first_resolution_time_in_minutes_within_business_hours = %(first_reply_time_in_minutes_within_business_hours)s, "
                    "full_resolution_time_in_minutes = %(full_resolution_time_in_minutes)s, "
                    "full_resolution_time_in_minutes_within_business_hours = %(full_resolution_time_in_minutes_within_business_hours)s, "
                    "agent_wait_time_in_minutes = %(agent_wait_time_in_minutes)s, "
                    "agent_wait_time_in_minutes_within_business_hours = %(agent_wait_time_in_minutes_within_business_hours)s, "
                    "requester_wait_time_in_minutes = %(requester_wait_time_in_minutes)s, "
                    "requester_wait_time_in_minutes_within_business_hours = %(requester_wait_time_in_minutes_within_business_hours)s, "
                    "on_hold_time_in_minutes = %(on_hold_time_in_minutes)s, "
                    "on_hold_time_in_minutes_within_business_hours = %(on_hold_time_in_minutes_within_business_hours)s, "
                    "datasift_username = %(datasift_username)s, "
                    "category = %(category)s,"
                    "status = %(status)s"
    )

    logging.info("Found {0} updated tickets.".format(len(tickets['results'])))

    for t in tickets['results']:
        # Process all date fields
        created_at = ConvertToUTC(t['created_at'])
        updated_at = ConvertToUTC(t['updated_at'])
        assigned_at = ConvertToUTC(t['assigned_at'])
        due_date = ConvertToUTC(t['due_date'])
        initially_assigned_at = ConvertToUTC(t['initially_assigned_at'])
        solved_at = ConvertToUTC(t['solved_at'])

        ticket_data = {
                'ticket_id': t['id'],
                'generated_timestamp': t['generated_timestamp'],
                'req_name': t['req_name'],
                'req_id': t['req_id'],
                'req_external_id': t['req_external_id'],
                'req_email': t['req_email'],
                'submitter_name': t['submitter_name'],
                'assignee_name': t['assignee_name'],
                'group_name': t['group_name'],
                'subject': t['subject'],
                'current_tags': t['current_tags'],
                'priority': t['priority'],
                'via': t['via'],
                'ticket_type': t['ticket_type'],
                'created_at': created_at,
                'updated_at': updated_at,
                'assigned_at': assigned_at,
                'organization_name': t['organization_name'],
                'due_date': due_date,
                'initially_assigned_at': initially_assigned_at,
                'solved_at': solved_at,
                'resolution_time': t['resolution_time'],
                'satisfaction_score': t['satisfaction_score'],
                'group_stations': t['group_stations'],
                'assignee_stations': t['assignee_stations'],
                'reopens': t['reopens'],
                'replies': t['replies'],
                'first_reply_time_in_minutes': t['first_reply_time_in_minutes'],
                'first_reply_time_in_minutes_within_business_hours': t['first_reply_time_in_minutes_within_business_hours'],
                'first_resolution_time_in_minutes': t['first_resolution_time_in_minutes'],
                'first_resolution_time_in_minutes_within_business_hours': t['first_resolution_time_in_minutes_within_business_hours'],
                'full_resolution_time_in_minutes': t['full_resolution_time_in_minutes'],
                'full_resolution_time_in_minutes_within_business_hours': t['full_resolution_time_in_minutes_within_business_hours'],
                'agent_wait_time_in_minutes': t['agent_wait_time_in_minutes'],
                'agent_wait_time_in_minutes_within_business_hours': t['agent_wait_time_in_minutes_within_business_hours'],
                'requester_wait_time_in_minutes': t['requester_wait_time_in_minutes'],
                'requester_wait_time_in_minutes_within_business_hours': t['requester_wait_time_in_minutes_within_business_hours'],
                'on_hold_time_in_minutes': t['on_hold_time_in_minutes'],
                'on_hold_time_in_minutes_within_business_hours': t['on_hold_time_in_minutes_within_business_hours'],
                'url': t['url'],
                'datasift_username': t['field_20729302'],
                'category': t['field_20409116'],
                'status': t['status']
            }
        logging.debug("Database data: {0}".format(ticket_data))
        mysqlDb.execute_query(dbQuery, ticket_data)

    logging.info("Completed pulling tickets from Zendesk. End time is: {0}".format(tickets['end_time']))

    logging.info("INCREMENTAL_EXPORT: end_time={0}".format(tickets['end_time']))
    
    mysqlDb.update_job_timestamp("INCREMENTAL_EXPORT", str(tickets['end_time']))
    
    logging.info("Done exporting Zendesk ticket updates.")

