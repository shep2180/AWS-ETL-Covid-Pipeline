# necessary libraries, boto3 is for AWS resources
import csv
import os
import json
import boto3

# imports functions from ETL module I created

from module_etl import get_data, extract_nyt, extract_jh, join_data, load_data

# setting up DynamoDB
dynamodb = boto3.resource('dynamodb')
table_name = os.environ['TABLE_NAME']
table = dynamodb.Table(table_name)

# setting up SNS
notifier = boto3.client('sns')
notifier_arn = os.environ['NOTIFIER_ARN']

def lambda_handler(event, context):
    
    # ETL process
    nyt_data, nyt_exceptions = extract_nyt(get_data('https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'))
    jh_data, jh_exceptions = extract_jh(get_data('https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'))
    joined_data = join_data(nyt_data, jh_data)
    rows_updated, load_exceptions = load_data(joined_data, table)
    
    # SNS notification
    result = {
        'nyt_exceptions': len(nyt_exceptions),
        'jh_exceptions': len(jh_exceptions),
        'load_exceptions': len(load_exceptions),
        'rows_updated': rows_updated,
    }
    
    notification = notifier.publish(
        TargetArn = notifier_arn,
        Message = json.dumps({'default': json.dumps(result)}),
        MessageStructure = 'json'
    )
    
    # Lambda return
    return {
        'statusCode': 200,
        'body': notification,
    }

