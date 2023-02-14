import datetime
from flickplay.mongo import get_mongo_db_obj
from flickplay.get_connections import get_alchemy_con, get_pymysql_con
from flickplay.utils import timer_func
import os
import pandas as pd
import numpy as np
import sys


__all__ = [
    "SQLFactory",
    "QueryEngine",
    "Devices",
    "Users",
    "Registrations",
    "CustomerChurn",
    "CustomerRetention"
    ]

class SQLFactory(object):
    @classmethod
    def get_pymysql_con(cls):
        return get_pymysql_con()
    
    @classmethod
    def get_alchemy_con(cls):
        return get_alchemy_con()
    
    DEFAULT_QUERY = """
    SHOW TABLES FROM fpa.events;
    """
    
    def __init__(self):
        self.query = self.DEFAULT_QUERY
        self.conp = self.get_pymysql_con()
        self.con = self.get_alchemy_con()
        
    def set_query(self, new_query=None):
        if new_query is None:
            self.query = self.DEFAULT_QUERY
        else:
            self.query = new_query     
    
    @timer_func
    def get(self):
        return pd.read_sql(self.query, self.conp)
      

class QueryEngine(SQLFactory):
    def __init__(self):
        super().__init__()
        self.set_query()
    

class Devices(QueryEngine):
    DEFAULT_QUERY = """
    SELECT 
        device, 
        count(*) cnt
    FROM 
        (select * from fpa.users where lastUsed>='2022-1-1') x
    GROUP BY device
    HAVING device is not null
    ORDER BY cnt DESC;"""
    def __init__(self):
        super().__init__()

    
class Users(QueryEngine):
    DEFAULT_QUERY = """
    SELECT *
    FROM fpa.users;
    """
    def __init__(self):
        super().__init__()

        
class Registrations(QueryEngine):
    DEFAULT_QUERY="""
    SELECT 
        month(created) as month_,
        year(created) as year_,
        sum(appleSignup) apple,
        sum(walletSignup) wallet,
        sum(regularSignup) regular,
        sum(anonymous) anon,
        sum(unclassifiedSignup) unclassified
    FROM
        fpa.users
    GROUP BY 
        year_, month_
    ORDER BY
        year_, month_ ASC;"""
    
    def __init__(self):
        super().__init__()
        
        
class CustomerChurn(QueryEngine):
    DEFAULT_QUERY= """
    SELECT
        year(lastUsed) last_yr,
        month(lastUsed) last_mo,
        ownsCertifiedNFT,
        --isWeb3,
        - count(user_id) churned

    FROM 
        ( select * from fpa.users where (anonymous=0 or anonymous is null) and unclassifiedSignup=0) x

    GROUP BY
        last_mo, last_yr, ownsCertifiedNFT, isWeb3

    HAVING 
        last_yr < 2023
    
    ORDER BY 
        last_yr, last_mo;"""
    
    def __init__(self):
        super().__init__()
        

class CustomerRetention(QueryEngine):
    DEFAULT_QUERY= """
    SELECT
        year(created) c_yr,
        month(created) c_mo,
        ownsCertifiedNFT,
        --isWeb3,
        count(user_id) joined

    FROM 
        ( select * from fpa.users where (anonymous=0 or anonymous is null) and unclassifiedSignup=0) x

    GROUP BY
        c_mo, c_yr, ownsCertifiedNFT

    HAVING 
        c_yr < 2023

    ORDER BY 
        c_yr, c_mo;"""

    def __init__(self):
        super().__init__()
        
    