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

import pandas as pd


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '10layhjfNXaj337SibJt2ZQn0Uzf4jojaCRuYuZzMB9Y'
# SAMPLE_RANGE_NAME = 'Hardware show 2020!A4:F'
# https://docs.google.com/spreadsheets/d/1YI56gLZFoNVkMtrup3mlN-BeBzygc-TqV5PaKiJe3bE/edit#gid=0

def read_write_google_sheet():

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

    RANGE_NAME = 'CoStar!A1:C'
    result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    values = result.get('values', [])

    company_links = []
    if not values:
        print('No data found.')
    else:
        
        for row in values:
            # print('%s' % (row[0]))
            company_links.append(row)

    return service, company_links    

def write_google_sheet_website(service, company_links):

    values = []
    for i in range(len(company_links)):
        if i < 3327:
            continue

        website = scrape_company_page(company_links[i])
        print(company_links[i], website)
        values.append([website])
        
        if (i+1) % 3 == 0:
            body = {'values': values }
            RANGE_NAME = 'Expo West!C'+str(i+2)+':C'
            result = service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, 
                valueInputOption='USER_ENTERED', body=body).execute()
            print('{0} cells updated.'.format(result.get('updatedCells')))
            values = []

def write_google_sheet(service, row_num, data):

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

    values = []
    values.append([data])
    body = {'values': values }
    RANGE_NAME = 'CoStar!C'+str(row_num)+':C'
    print(RANGE_NAME, row_num, body)
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
        valueInputOption='USER_ENTERED', body=body).execute()
    print('{0} cells updated.'.format(result.get('updatedCells')))
        # values = []

def addCommatoString(str):
    if str != "":
        str = "\""+str+"\""
    return str

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


def findFromCSV(filename, key_row):

	result = ''
	with open(filename, newline='\n') as csvfile:
		spamreader = csv.reader(csvfile, delimiter=',')
		all_rows = []
		modified_address = modifiedAddress(key_row[0])
		print(modified_address)
		for row in spamreader:
			if (row[2].lower() == key_row[0].lower() or row[2].lower() == modified_address.lower()) and row[3] == key_row[1]:
				result = row[0]
				break
			# for column in row:
			# 	index = index + 1
			# 	columns.append(addCommatoString(column))
			# 	if index == 19:
			# 		print(columns)
			# 		all_rows.append(columns)
			# 		index, columns = 0, []
			# 		rows = rows + 1

				# if rows == 25:
				# 	break

	return result

def adjustAddress(address):
	address = address.replace('0th', '0').replace('1st', '1').replace('2nd', '2').replace('3rd', '3').replace('4th', '4').replace('5th', '5').replace('6th', '6').replace('7th', '7').replace('8th', '8').replace('9th', '9')

	return address
# f = open('result.csv', 'a', newline='', encoding='utf-8')
# writer = csv.writer(f)
# for row in all_rows:
#     f.write(','.join([str(elem) for elem in row])+"\n")

if __name__ == '__main__':
	service, data = read_write_google_sheet()
	
	row_num = 0
	for row in data:
		row_num = row_num + 1
		# if row_num <= 155: continue
		if len(row) == 3: continue
		row[0] = adjustAddress(row[0])
		row[1] = row[1].split("-")[0]
		print(row)
		source = findFromCSV("all.csv", row)
		if source != "": 
			write_google_sheet(service, row_num, source)
			print(source)
		# break
	
	# print(row_num)
