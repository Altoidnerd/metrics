from collections import Counter
import datetime
import json
import matplotlib.pyplot as plt
from os.path import abspath, isdir, isfile, join
from os import mkdir
import pandas as pd
import requests
import sys

from secrets import secrets

auth = secrets.get('mixpanel').get('auth')

pd.set_option('max_columns', None)
# pd.set_option('display.max_rows', None)


def get_data_between(start_date=None, 
                     end_date=None,
                     verbose=False):

    print('Doing API call to mixpanel.')
    
    if start_date is None or end_date is None:
        start_date = '2022-1-1'
        end_date   = '2022-1-7'
    
    url = f'https://data.mixpanel.com/api/2.0/export?from_date={start_date}&to_date={end_date}'
    headers = {
        "accept": "text/plain",
        "authorization": f"{auth}"
    }
    print(f'Getting {url} with \n{headers}')
    response = requests.get(url, headers=headers)
    
    verbose and print(response.text[:200])
    result = {'events': []}
    events = response.text.strip().split('\n')
    
    # with open('~/Desktop/EVENTS/err.log', 'a') as f:
    #     f.write(str(start_date)+':\n')
    #     f.write(response.text)
    print(f'Response received. Processing events between {start_date} and {end_date}.')
    for event in events:
        result['events'].append(json.loads(event))


    # Flatten all events
    print(f'''{len(result['events'])} events received.''')
    data =  [ _['properties'] for _ in result['events']] 
    df = pd.DataFrame.from_records(data)

    # this df is missing the event names
    # df
    def humanize_timestamp(ts):
        ts = int(ts)
        return datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    # changing unix timestamp to stuff that is useful
    # a human readable date string
    df['date'] = [ humanize_timestamp(ts) for ts in df.time ] 
    # a python datetime format
    df.datetime = [ datetime.datetime.utcfromtimestamp(ts) for ts in df.time ]
    # the event names are here:
    event_names = [ _['event'] for _ in result['events']]
    # reorder the columns
    old_columns = list(df.columns)
    df['event'] = event_names
    newcolumns = ['event', 'date'] + old_columns
    df = df[newcolumns]
    
    print(f'Flattened data has {len(df)} rows.')
    return df


def do_histogram(df,
                 save_as=None):
    
    '''
    If save is None, no plot will be saved;
    else, pass a string and itll save to that path.
    '''
    
    print('Building histogram')
    
    c = dict(Counter(df.event).most_common(20))
    c.pop('performance__network', None)
    fig, ax = plt.subplots(figsize=(20, 7))
    # plt.rcParams["figure.figsize"] = [20, 7]
    # plt.rcParams["figure.autolayout"] = True
    plt.xticks(rotation=70)
    plt.yscale('log')
    plt.title('Frequency of most common user events in November')
    plt.bar(c.keys(), c.values())
    plt.show()
    
    if save_as:
        outfile = join(abspath('img'), save_as)
        if len(outfile.split('.'))>=1:
            astype = outfile.split('.')[-1]
        else:
            astype = 'pdf'
            outfile = outfile+'.pdf'
            
        plt.savefig(outfile,
                    dpi=300,
                    format=astype)
    
    return plt

    
odfs = []
    
def main(outdir='data'):

    i_date = datetime.date(2022,3,13)
    dt = datetime.timedelta(days=1)

    for i in range(332):
        new_date = (i_date+dt*i).isoformat()
        print(f'Doing date: {new_date}.')
        try:
            df = get_data_between(start_date=new_date,
                                  end_date=new_date,
                                  verbose=1)

            do_histogram(df)
            
            if not isdir(outdir):
                if not isdir(abspath(outdir)):
                    print(f'Making {abspath(outdir)}')
                    mkdir(abspath(outdir))
            
            outfile = join(abspath(outdir), f'{new_date}.csv')
            print(f'Saving to {outfile}.') # defaults to ./data/
            df.to_csv(outfile)
            odfs.append(df)
            print(len(odfs), 'many dfs')

        except:
            log = {}
            log['date'] = new_date
            log['e'] = f'Exception occured trying to fetch {new_date}.'
            log['info'] = str(sys.exc_info())
            continue

    
    
# outdir = 'data'
# i_date = datetime.date(2022,4,12)
# dt = datetime.timedelta(days=1)

# # dfs = [] 

# for i in range(300):
#     start_date = (i_date + dt*i).isoformat()
#     end_date = start_date
    
#     print(start_date)
#     try:
#         df = get_data_between(start_date=start_date, 
#                               end_date=end_date,
#                               verbose=1)

#         devices = dict(Counter(df['$model']).most_common(20))
#         devices_users = dict()
#         devices_N_users = dict()

#         outfile = join(abspath(outdir), f'{start_date}.csv')
#         print(f'Saving to {outfile}.') # defaults to ./data/
#         df.to_csv(outfile)
#         odfs.append(df)
#         print(len(odfs), 'many dfs')

#         dfs.append(df)
#         # plt.rcParams["figure.figsize"] = [20, 7]
#         # plt.rcParams["figure.autolayout"] = True
#         plt.xticks(rotation=70)
#         plt.yscale('log')
#         plt.title(f'Frequency of most common devices on {start_date}.')
#         plt.bar(devices.keys(), devices.values())
#         plt.show()
#         print('Number of dfs:',len(dfs))
    
#     except:
#         continue
        
        
# main()
    
#     df = get_data_between(start_date=new_date,
#                           end_date=new_date)

#     do_histogram(df)

#     outfile = f'~/Desktop/EVENTS/{new_date}.csv'
#     print(f'Saving to {outfile}.')
#     df.to_csv(outfile)

dfs = []
    
outdir = 'data'
i_date = datetime.date(2022,2,1)
dt = datetime.timedelta(days=1)

# dfs = [] 

for i in range(365):
    start_date = (i_date + dt*i).isoformat()
    end_date = start_date
    
    if isfile(f'./data/{start_date}.csv'):
        print(f'./data/{start_date}.csv exists; skipping.')
        continue
    
    print(start_date)
    try:
        df = get_data_between(start_date=start_date, 
                              end_date=end_date,
                              verbose=1)

        devices = dict(Counter(df['$model']).most_common(20))
        devices_users = dict()
        devices_N_users = dict()

        outfile = join(abspath(outdir), f'{start_date}.csv')
        print(f'Saving to {outfile}.') # defaults to ./data/
        df.to_csv(outfile)
        odfs.append(df)
        print(len(odfs), 'many dfs')

        dfs.append(df)
        # plt.rcParams["figure.figsize"] = [20, 7]
        # plt.rcParams["figure.autolayout"] = True
        plt.xticks(rotation=70)
        plt.yscale('log')
        plt.title(f'Frequency of most common devices on {start_date}.')
        plt.bar(devices.keys(), devices.values())
        plt.show()
        print('Number of dfs:',len(dfs))
    

    except:
        continue
    
    