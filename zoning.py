import csv
import os
import sys
from datetime import datetime
# import requests
import re
import json
from urllib.parse import urlparse

# # import settings
import logging
logging.getLogger().setLevel(logging.INFO)
import time

import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import requests
from requests.exceptions import HTTPError

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '10layhjfNXaj337SibJt2ZQn0Uzf4jojaCRuYuZzMB9Y'

def read_Final_google_sheet():

	creds = None
	# The file token.pickle stores the user's access and refresh tokens, and is
	# created automatically when the authorization flow completes for the first
	# time.
	if os.path.exists('token.pickle'):
		with open('token.pickle', 'rb') as token:
			creds = pickle.load(token)
	# If there are no (valid) credentials available, let the user log in.
	if not creds or not creds.valid:
		if creds and creds.expired and creds.refresh_token:
			creds.refresh(Request())
		else:
			flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
			creds = flow.run_local_server()
		# Save the credentials for the next run
		with open('token.pickle', 'wb') as token:
			pickle.dump(creds, token)
	service = build('sheets', 'v4', credentials=creds)

	RANGE_NAME = 'Final!A2:A'
	result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
	values = result.get('values', [])

	data = []
	if not values:
		print('No data found.')
	else:
		
		for row in values:
			# print('%s' % (row[0]))
			data.append(row[0])

	return service, data    

def read_CoStar_google_sheet():

    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    service = build('sheets', 'v4', credentials=creds)

    RANGE_NAME = 'CoStar1!A2:I'
    result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    values = result.get('values', [])

    data = []
    if not values:
        print('No data found.')
    else:
        
        for row in values:
            # print('%s' % (row[0]))
            data.append(row)

    return service, data   

def write_google_sheet(service, data):

    if service == None:
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server()
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        service = build('sheets', 'v4', credentials=creds)
    
    # print(values)
    body = {'values': data }
    RANGE_NAME = 'Zoning!A2:M'
    # print(RANGE_NAME, body)
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
        valueInputOption='USER_ENTERED', body=body).execute()
    print('{0} cells updated.'.format(result.get('updatedCells')))

def write_BBLE_google_sheet(service, row_num, data):

    if service == None:
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server()
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        service = build('sheets', 'v4', credentials=creds)

    row = []
    values = []

    keys = ['bble']
    for key in keys:
        if key in data.keys():
            value = data[key]
            if key == "geocoded_column":
                print(type(value))
                row.append(value['type']+" "+str(value['coordinates']))
            else:
                row.append(value)
        else:
            row.append('')
    
    values.append(row)
    body = {'values': values }
    RANGE_NAME = 'CoStar1!B'+str(row_num)+':B'
    # print(RANGE_NAME, row_num, body)
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
        valueInputOption='USER_ENTERED', body=body).execute()
    print('{0} cells updated.'.format(result.get('updatedCells')))


if __name__ == '__main__':
	
	service, BBLEs = read_Final_google_sheet()
	# print(BBLEs)
	
	results = []
	with open('NY_ZoningTaxLotDB.csv') as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=',')
		for row in csv_reader:
			if row[3] in BBLEs:
				results.append(row[3:])

	write_google_sheet(service, results)
	