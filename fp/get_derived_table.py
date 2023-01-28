from get_connections import get_pymysql_con

import datetime
from datetime import timedelta
import glob
import warnings
import pymysql



def get_dervied_from_fpa_events():
    conp = get_pymysql_con()

    query = '''
    SELECT
        event,
        date,
        time,
        $user_id,
        user_gps_location
    FROM
        fpa.events
    -- WHERE
    --  date>="2022-05-01"'''

    print(f'Fetching query.')
    print(query)
    dd = pd.read_sql(query, conp)


    print('Mapping months.')
    months = []
    for idx, date in enumerate(dd.date):
        months.append((str(date)+'-z-z-').split('-')[1])
        if idx%100000==0:
            sys.stdout.write(f'{idx}\r')
    dd['month'] = months



    # do days, weeks
    dt = timedelta(days=1)
    day_mapper = [ (datetime.datetime(2022,1,1)+dt*i).date() for i in range(365) ]

    def get_day_by_date(date, mapper=day_mapper):
        if mapper is None:
            mapper = [ (datetime.datetime(2022,1,1)+dt*i).date() for i in range(365) ]
        return day_mapper.index(date)+1

    print('Mapping days')
    dd['day'] = [ get_day_by_date(d) for d in dd.date ]


    def get_week_by_date(date):
        return (get_day_by_date(date)-1)//7+1


    print('Mapping weeks.')
    weeks = []
    for idx, date in enumerate(dd.date):
        try:
            weeks.append( get_week_by_date(date))
        except:
            weeks.append(-1)
            print(sys.exc_info())

        if idx%100000==0:
            sys.stdout.write(f'{idx}\r')

    dd['week'] = weeks

    dd['dt'] = [ datetime.datetime.fromtimestamp(ts) for ts in dd.time ]

    dd['hour'] = [ t.hour for t in dd.dt ]
    
    return dd


def replace_derived(new_derived_data: pd.DataFrame, 
                    target_table_name='derived',
                    target_database='fpa',
                    chunks='default')-> None:
    
    con = get_alchemy_con(database=target_database)
    
    table_name = target_table_name

    dd = get_dervied_from_fpa_events()
    
    cdd = dd.iloc[0:0]

    print('Create schema')
    cdd.to_sql(table_name,
              con,
              if_exists='replace',
              index=False)

    # set shunks
    if chunks == 'default':
        chunksize = 100000
        chunkno = 1
        upper = chunksize
        lower=0

    else:
        chunksize, chunkno, upper, lower == chunker(chunks=chunks)
        
    # do upload by chunks
    while upper < len(dd)+chunksize:

        lower+=chunksize
        upper+=chunksize
        upper = min(len(dd), upper)

        # get chunk
        dd_chunk = dd.iloc[lower:upper]

        print('Uploading:', chunksize, chunkno, upper, lower)
        
        dd_chunk.to_sql('derived',
               con,
               if_exists='append',
               index=False)

        if upper - lower < chunksize:
            break

        chunkno+=1