from pymongo import MongoClient
import pandas as pd
from secrets import secrets

u = secrets.get('mongo').get('user')
p = secrets.get('mongo').get('password')


def get_mongo_client(target_db='flickplay-production'):
    u = secrets.get('mongo').get('user')
    p = secrets.get('mongo').get('password')
    con_string = f'mongodb+srv://{u}:{p}@main-4-4.rph7u.mongodb.net/'
    return MongoClient(con_string)
    
def get_mongo_db_obj(target_db='flickplay-production'):
    u = secrets.get('mongo').get('user')
    p = secrets.get('mongo').get('password')
    con_string = f'mongodb+srv://{u}:{p}@main-4-4.rph7u.mongodb.net/'
    client = MongoClient(con_string)
    targetdb = target_db
    return client[targetdb]


    

def main():
    
    print('Loading secrets ... ')
    from secrets import secrets
    u = secrets.get('mongo').get('user')
    p = secrets.get('mongo').get('password')

    con_string = f'mongodb+srv://{u}:{p}@main-4-4.rph7u.mongodb.net/'


    # targetdb = 'flickplay-development'
    targetdb = 'flickplay-production'

    print(f'Connecting to {targetdb}...')
    client = MongoClient(con_string)
    db = client[targetdb]
    print('Connected!')


    return db.users.find_one({'username': 'allenn'})

if __name__=='__main__':
    main()
    