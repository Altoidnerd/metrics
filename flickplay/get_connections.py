from abc import ABC, ABCMeta
from dataclasses import dataclass
import pandas as pd
import pymysql
import mysql.connector
from sqlalchemy import create_engine

############################################################
# DO NOT PUSH YOUR CREDS!!
# *secret*py is in the .gitignore, 
# so is never pushed.
# 
# You need to get secrets.py from Allen or Dima and,
# place it in flickplay/ dir, for a connection
# to be successful.
############################################################
from flickplay.secrets import secrets

__all__ = [
    'LocalConnector',
    'GCPConnector'
    ]

class Auth(ABC):
    pass


@dataclass
class GCPAuth(Auth):
    auth = secrets['jupyter-server-db']
    host = auth['host']
    user = auth['user']
    password = auth['password']
    database = 'fpa'
    creds = dict(host=host,user=user,password=password,database=database)

    
@dataclass
class LocalAuth(Auth):
    auth = secrets['localdb']
    host = auth['host']
    user = auth['user']
    password = auth['password']
    database = 'fpa'
    creds = dict(host=host,user=user,password=password,database=database)

    
class GCPConnector(GCPAuth): 
    def __init__(self):
        super().__init__()
        self.auth=GCPAuth()
        self.host=self.auth.host
        self.user=self.auth.user
        self.password=self.auth.password
        self.database=self.auth.database
        self.creds=self.auth.creds
        
    def get_alchemy_con(self, database='fpa'):
        return create_engine(f'mysql+pymysql://{self.user}:{self.password}@localhost/{self.database}')

    def get_pymysql_con(self, database='fpa'):
        self.creds['database'] = database
        return pymysql.connect(**self.creds)

    def get_mysql_connector_con(self, database='fpa'):
        self.creds['database'] = database
        return mysql.connector.connect(**self.creds)

class LocalConnector(object):
    def __init__(self):
        super().__init__()
        self.auth=LocalAuth()
        self.host=self.auth.host
        self.user=self.auth.user
        self.password=self.auth.password
        self.database=self.auth.database
        self.creds=self.auth.creds
        
    def get_alchemy_con(self, database='fpa'):
        return create_engine(f'mysql+pymysql://{self.user}:{self.password}@localhost/{self.database}')

    def get_pymysql_con(self, database='fpa'):
        self.creds['database'] = database
        return pymysql.connect(**self.creds)

    def get_mysql_connector_con(self, database='fpa'):
        self.creds['database'] = database
        return mysql.connector.connect(**self.creds)
    

def main():
    doc = '''
    User Classes:
    GCPConnector, LocalConnector
    
    Usage:
    con = GCPConnector().get_alchemy_con()
    print(pd.read_sql("""SHOW DATABASES;""", con))
    
    lcon = LocalConnector().get_pymysql_con()
    print(pd.read_sql("""SHOW DATABASES;""", con))
    '''
    print(doc)

    
if __name__ == '__main__':
    main()