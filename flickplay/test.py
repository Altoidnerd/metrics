import os
import pandas as pd
import datetime
from datetime import timedelta
import glob
import warnings
import pymysql
from mysql.connector import connect
import os
from sqlalchemy import create_engine
import sys
from dataclasses import dataclass
from secrets import secrets
auth = secrets.get('jupyter-server-db')

@dataclass
class GCPConnector(object):

    host=auth['host']
    user=auth['user']
    password=auth['password']
    # port=3306
    database='fpa'
    creds = dict(host=host,user=user,password=password,database=database)

    def get_gcp_alchemy_con(database='fpa'):
        host='localhost'
        user='root'
        password=''
        port=3306
        database=database
        return create_engine(f'mysql+pymysql://{user}:{password}@localhost/{database}')

    def get_gcp_pymysql_con(database='fpa'):
        creds['database'] = database
        return pymysql.connect(**creds)

    def get_gcp_mysql_connector_con(database='fpa'):
        creds['database'] = database
        return connect(**creds)


@dataclass
class LocalConnector(object):

    host='localhost'
    user='root'
    password=''
    port=3306
    database='fpa'
    creds = dict(host=host,user=user,password=password,database=database)

    def get_alchemy_con(database='fpa'):
        host='localhost'
        user='root'
        password=''
        port=3306
        database=database
        return create_engine(f'mysql+pymysql://{user}:{password}@localhost/{database}')

    def get_pymysql_con(database='fpa'):
        creds['database'] = database
        return pymysql.connect(**creds)

    def get_mysql_connector_con(database='fpa'):
        creds['database'] = database
        return connect(**creds)