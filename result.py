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

def read_Reonomy_google_sheet():

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

    RANGE_NAME = 'Reonomy1!A2:K'
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

    row = []
    values = []

    keys = ['bble', 'boro', 'block', 'lot', 'easement', 'owner', 'bldgcl', 'taxclass', 'ltfront', 'ltdepth', 'ext', 'stories', 'fullval', 'avland', 'avtot', 'exland', 'extot', 'excd1', 'staddr', 'zip', 'exmptcl', 'bldfront', 'blddepth', 'avland2', 'avtot2', 'exland2', 'extot2', 'excd2', 'period', 'year', 'valtype', 'borough', 'latitude', 'longitude', 'community_board', 'council_district', 'census_tract', 'bin', 'nta', 'geocoded_column']
    for bble, value in data.items():
        row.append(bble)
        if 'reonomy' in value.keys():
            row.append(','.join(value['reonomy']))
        else:
            row.append('')

        if 'costar' in value.keys():
            row.append(','.join(value['costar']))
        else:
            row.append('')
		
        values.append(row)
        row = []
        # print(row)
        # print(values)
    
    # print(values)
    body = {'values': values }
    RANGE_NAME = 'Final!A2:AZ'
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

def modifiedAddress(address):
	if address.split(" ")[0].isdigit():
		number = address.split(" ")[0]
		if len(number) >= 3:
			new_number = number[:-2]+"-"+number[-2:]
			address = address.replace(number, new_number)
	elif "-" in address.split(" ")[0]:
		number = address.split(" ")[0]
		new_number = number.replace("-", "")
		address = address.replace(number, new_number)
	
	return address

def adjustAddress(address):
	address = address.replace('0th', '0').replace('1st', '1').replace('2nd', '2').replace('3rd', '3').replace('4th', '4').replace('5th', '5').replace('6th', '6').replace('7th', '7').replace('8th', '8').replace('9th', '9')

	return address

def getDataFromNYCOpen(bble, year):

	try:
		response = requests.get('https://data.cityofnewyork.us/resource/yjxr-fw8i.json?bble='+bble+'&year=%27'+year+'%27')
		response.raise_for_status()
		jsonResponse = response.json()

		return jsonResponse

	except HTTPError as http_err:
		print(f'HTTP error occurred: {http_err}')
		exit()
	except Exception as err:
		print(f'Other error occurred: {err}')
		exit()

def getBBLEFromAddress(address, year):
	
	try:
		response = requests.get("https://data.cityofnewyork.us/resource/yjxr-fw8i.json?$where=UPPER(staddr)='"+address.upper()+"'&year=%27"+year+'%27')
		response.raise_for_status()
		jsonResponse = response.json()

		return jsonResponse

	except HTTPError as http_err:
		print(f'HTTP error occurred: {http_err}')
		exit()
	except Exception as err:
		print(f'Other error occurred: {err}')
		exit()

if __name__ == '__main__':
	
	service, reonomy_data = read_Reonomy_google_sheet()
	service, costar_data = read_CoStar_google_sheet()
	
	results = {}
	for reonomy in reonomy_data:
		# if reonomy[0] != "O": continue
		# print(reonomy[1], reonomy[4])
		if reonomy[4] in results.keys():
			results[reonomy[4]]['reonomy'].append(reonomy[1])
		else:
			results[reonomy[4]] = {'reonomy': [reonomy[1]]}

	for costar in costar_data:
		if costar[0] != "O": continue
		# print(reonomy[1], reonomy[4])
		
		if costar[1] in results.keys():
			if 'costar' in results[costar[1]]:
				results[costar[1]]['costar'].append(costar[2])
			else:
				results[costar[1]]['costar'] = [costar[2]]
		else:
			results[costar[1]] = {'costar': [costar[2]]}
	
	# print(results)
	write_google_sheet(service, results)
	