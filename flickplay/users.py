import datetime
from flickplay.mongo import get_mongo_db_obj
from flickplay.get_connections import LocalConnector
from flickplay.utils import timer_func
import os
import pandas as pd
import numpy as np
import sys

lc = LocalConnector()
conp, con = lc.get_pymysql_con(), lc.get_alchemy_con()

# wd='/Users/allen/Developer/clo/flickplay/mixpanel-events'
# os.chdir(wd)
# print(os.getcwd(), wd==os.getcwd())
# db, conp, con = get_mongo_db_obj(), get_pymysql_con(), get_alchemy_con()


@timer_func
def get_nfts(db=None):
    if db is None:
        db = get_mongo_db_obj()
    print('Getting NFTs from experiences')
    nfts = db.experiences.find({'nft': True, 'certified': True})
    return pd.DataFrame.from_records(nfts)

@timer_func
def get_recordings(db=None):
    if db is None:
        db = get_mongo_db_obj()
    print('Getting recordings from recordings')
    return pd.DataFrame.from_records(db.recordings.find())

@timer_func
def get_certified_recordings(dr=None):
    if dr is None:
        dr = get_recordings()
    print('Construct DataFrame of Certified Recordings (dcr)')
    dcrids = []
    for idx, row in dr.iterrows():
        recording_has_certified_nft = False
        for e in row.experiences:
            if e in certified_nft_ids:
                dcrids.append(row._id)
                continue
    dcr = dr[dr._id.isin(dcrids)]
    dcr['month'] = dcr['created'].apply(lambda x: x.date().month)
    dcr['year'] = dcr.created.apply(lambda x: x.date().year)
    dcr['month'] = dcr.created.apply(lambda x: x.date().month)
    dcr['date'] = dcr.created.apply(lambda x: str(x.date().year)+'-'+str(x.date().month))
    return dcr

@timer_func
def get_users():
    print('Getting users from users')
    return pd.DataFrame(db.users.find())

@timer_func
def get_recording_aggregates(dcr=None):
    if dcr is None:
        dcr = get_certified_recordings()
        
    print('Doing Recordings Aggregates')
    recs = []
    for year in [2020,2021,2022,2023]:
        for month in [i+1 for i in range(12)]:
            if year == 2020:
                if month < 12:
                    continue
            if year == 2023:
                if month > 1:
                    continue
            date        = str(year)+'-'+str(month)
            nrecordings = len(dcr[(dcr.year==year) & (dcr.month==month)])
            recordings  = list(set(dcr[(dcr.year==year) & (dcr.month==month)]._id))
            nusers      = len(set(dcr[(dcr.year==year) & (dcr.month==month)].user))
            users       = list(set(dcr[(dcr.year==year) & (dcr.month==month)].user))
            rec         = {
                            'date': date,
                            'year': year,
                            'month': month,
                            'recordings': recordings,
                            'n_recordings': nrecordings,
                            'n_users': nusers,
                            'users': users }
            recs.append(rec)
    aggregates = pd.DataFrame.from_records(recs)
    return aggregates

@timer_func
def get_process_recordings(dcr=None, dr=None):
    if dr is None:
        dr = get_recordings()
    if dcr is None:
        dcr=get_certified_recordings(dr=dr)
    recordings_with_certified_nft = dcr
    print('Processing Recordings')
    dr['hasCertifiedNFT'] = [ True if row._id in set(recordings_with_certified_nft._id.values) else False for idx, row in dr.iterrows() ]
    return dr

@timer_func
def get_web3_cohort():
    query = """select distinct($user_id) from fpa.events where event='profile_wallet__connect_suc'"""
    return pd.read_sql(query, con)


@timer_func
def get_process_users_(nfts=None,
                       users=None,
                       web3=None):
    if nfts is None:
        nfts = get_nfts()
    if users is None:
        users = get_users()
    if web3 is None:
        web3 = get_web3_cohort()
        
    print('Processing User hasCertifiedNFT')
    nftOwners = set(nfts.nftOwner)
    users['ownsCertifiedNFT'] = users._id.apply(lambda x: x in nftOwners)
    web3 = set(get_web3_cohort()['$user_id'])
    users['web3Cohort'] = users._id.apply(lambda x: 1 if x in web3 else 0)
    return users

def process_reg_type(user):
    if user.anonymous:
        return 'anonymous'
    elif user.createdThroughConnectWallet:
        return 'wallet connect'
    elif not np.isnan(user.appleId):
        return 'apple'
    else:
        return 'regular'

def get_registration_filter(kind='apple'):
    if kind not in ('apple', 'walletConnect', 'regular', 'anonymous'):
        raise ValueError(f"kind must be one of 'apple', 'walletConnect', 'regular', 'anonymous'")
    if kind == 'apple':
        return {'appleId': {'$ne': None}}
    elif kind == 'walletConnect':
        return {'createdThroughConnectWallet': True}
    elif kind == 'anonymous':
        return {'anonymous': True}
    else:
        return  {'anonymous': False, 'createdThroughConnectWallet': False, 'appleId': None}
    
@timer_func
def get_users_by_reg_type(kind='apple'):
    reg_filter = get_registration_filter(kind=kind)
    return pd.DataFrame.from_records(db.users.find(reg_filter))

@timer_func
def map_appleSignup(u):
    uids = set(userids_by_reg_type['apple'])
    u['appleSignup'] = u._id.apply(lambda x: int(x in uids))
    return u

@timer_func
def map_walletSignup(u):
    uids = set(userids_by_reg_type['walletConnect'])
    u['walletSignup'] = u._id.apply(lambda x: int(x in uids))
    return u

@timer_func
def map_regularSignup(u):
    uids = set(userids_by_reg_type['regular'])
    u['regularSignup'] = u._id.apply(lambda x: int(x in uids))
    return u

@timer_func
def map_strUserId(u):
    u['user_id'] = u._id.apply(lambda x: str(x))
    return u

@timer_func
def get_process_users(nfts=None, u=None, db=None):
    if db is None:
        db = get_mongo_db_obj()
    
    if nfts is None:
        nfts = get_nfts()
    
    if u is None:
        u = get_process_users_(nfts=nfts)
              
    apple = get_users_by_reg_type('apple')
    regular = get_users_by_reg_type('regular')
    walletConnect = get_users_by_reg_type('walletConnect')
    anonymous = get_users_by_reg_type('anonymous')

    users_by_reg_type = {
        'all': u,
        'apple': apple,
        'regular': regular,
        'walletConnect': walletConnect,
        'anonymous': anonymous
    }
    userids_by_reg_type = { k:df._id.values for k, df in users_by_reg_type.items() }
    
    u = get_process_users_(nfts=nfts).infer_objects().copy()

    def map_bool_to_int(colname, userdf):
        return userdf[colname].apply(lambda x: int(x) if type(x)==bool else x)

    def kill_nan(colname, userdf):
        return userdf[colname].apply(lambda x: None if np.isnan(x) else x)

    for col in u1.columns:
        print(col)
        try:
            u1[col] = map_bool_to_int(col, u1)
            u1[col] = kill_nan(col, u1)
        except Exception as e:
            print(e)
            
    return u.infer_objects()