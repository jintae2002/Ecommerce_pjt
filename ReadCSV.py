import json
import os
import boto3
import csv
import time
import numpy as np
import pandas as pd
import pymysql
from sqlalchemy import create_engine
pymysql.install_as_MySQLdb()
import MySQLdb

def lambda_handler(event, context):
    print(event['key1'])
    print(event['key2'])

    key = 'Global-Superstore.csv'
    # key = 'Brazil_Ecommerce.csv'
    # key = 'kaggle_Ecommerce.csv'

    bucket = 'gumegume'

    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucket, key)
    print(s3_object)
    data = s3_object.get()['Body'].read().decode('utf-8').splitlines()

    lines = csv.reader(data)

    headers = next(lines)

    print('headers: %s' % (headers))
    # for line in lines:
    #     print(line)
        # print(line[0], line[1])
        
    df_lines = pd.DataFrame(lines, columns=headers)
    df_lines['Order Date'] = pd.to_datetime(df_lines['Order Date'], format='%Y-%m-%d')
    # df_lines['InvoiceDate'] = pd.to_datetime(df_lines['InvoiceDate'], format='%m/%d/%Y %H:%M')
    print(df_lines)
    
    engine = create_engine("mysql+mysqldb://jintae2002:"+"medici0330"+"@database-2.c1udiifin44z.ap-northeast-2.rds.amazonaws.com/kumekume", encoding='utf-8')
    
    conn = engine.connect()
    
    # df_lines.to_sql(name='Gcommerce_log0', con=engine)
    # df_lines.to_sql(name='Bcommerce_log0', con=engine)
    # df_lines.to_sql(name='Kcommerce_log0', con=engine)

    
    return event['key1'], event['key2']