# necessary libraries
from urllib import request
import datetime
import csv

# downloads a dataset from provided url
def get_data(data_url):
    download = request.urlopen(data_url)
    data = download.read().decode('utf-8')
    
    return data

# converts the NYT date strings to datetime objects
def extract_nyt(data):
    
    cases = dict()
    exceptions = []
    
    for row in data.split('\n')[1:]:
        try:
            fields = row.split(',')
            format_date = datetime.datetime.strptime(fields[0], '%Y-%m-%d').date()
            cases[format_date] = fields[1:]
        except Exception as e:
            exceptions.append((row, str(e)))
    
    return cases, exceptions

# pulls the relevant US recovered data from JH
def extract_jh(data):
    
    recovered = dict()
    exceptions = []
    
    for row in data.split('\n')[1:]:
        try:
            if ',US,' in row:
                fields = row.split(',')
                date_ = date_time_obj = datetime.datetime.strptime(fields[0], '%Y-%m-%d').date()
                recovered_count = fields[-2]
                
                recovered[date_] = recovered_count
        except Exception as e:
            exceptions.append((row, str(e)))
    
    return recovered, exceptions


# necessary libraries
from urllib import request
import datetime
import csv

# downloads a dataset from provided url
def get_data(data_url):
    download = request.urlopen(data_url)
    data = download.read().decode('utf-8')
    
    return data

# converts the NYT date strings to datetime objects
def extract_nyt(data):
    
    cases = dict()
    exceptions = []
    
    for row in data.split('\n')[1:]:
        try:
            fields = row.split(',')
            format_date = datetime.datetime.strptime(fields[0], '%Y-%m-%d').date()
            cases[format_date] = fields[1:]
        except Exception as e:
            exceptions.append((row, str(e)))
    
    return cases, exceptions

# pulls the relevant US recovered data from JH
def extract_jh(data):
    
    recovered = dict()
    exceptions = []
    
    for row in data.split('\n')[1:]:
        try:
            if ',US,' in row:
                fields = row.split(',')
                date_record = date_time_obj = datetime.datetime.strptime(fields[0], '%Y-%m-%d').date()
                recovered_count = fields[-2]
                
                recovered[date_record] = recovered_count
        except Exception as e:
            exceptions.append((row, str(e)))
    
    return recovered, exceptions


# joins JH and NY Times data 
def join_data(nyt_data, jh_data):

    combined_data = []

    for date_record in nyt_data.keys():
        if date_record in jh_data:
            row = []
            row.append(str(date_record))
            row += nyt_data[date_record]
            row.append(jh_data[date_record])
            combined_data.append(row)
    
    return combined_data

# loads data into a dynamodb nosql database
def load_data(data, table):

    rows_updated = 0
    exceptions = []
    
    for row in data:
        try:
            date_record, cases, deaths, recovered = row
            table.put_item(Item = 
                          {
                              'id': date_record,
                              'cases': int(cases),
                              'recovered': int(recovered),
                              'deaths': int(deaths)
                          },
            ConditionExpression = 'attribute_not_exists(date_record)')
            rows_updated = rows_updated + 1
            
        except Exception as e:
            exceptions.append((row, str(e)))
            
    return rows_updated, exceptions

# TODO 
#       -SNS notifications for errors and successful additions