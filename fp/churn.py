from flickplay.mongo import get_mongo_db_obj
from flickplay.get_connections import get_alchemy_con, get_pymysql_con
from flickplay.utils import *
import pandas as pd

# con = get_alchemy_con()

class GlobalSettings:
    VERBOSE = True
    KINDS = ['usersWithCertifiedNFT',
             'usersWithoutCertifiedNFT']
    
    plotno=0
    
    @classmethod
    def set_verbose(cls, new_verbosity):
        cls.VERBOSE = new_verbosity
        
    @classmethod
    def get_plotno(cls):
        cls.plotno+=1
        return cls.plotno-1


def stringify_pct(pct):
    if type(pct) == float:
        return '+'+str(round(pct,2))+'%' if pct>=0 else str(round(pct,2))+'%'
    else:
        return pct
    
def unstringify_pct(spct):
    if type(spct)==str:
        return round(float(spct.replace('+','').replace('%','')),6)
    else:
        return spct

def transform_sql_response(
              data, 
              stringify=True,
              all_columns=False,
              truncate_larger=True,
              backshift=True,
              n_backshift=26):

    d1=data.copy()
    d1['net_change']       =   [ row.joined + row.churned for i, row in d1.iterrows() ]
    d1['cum_joined']       =   [ sum(d1.joined.values[0:i])+d1.iloc[i].joined for i in range(len(d1)) ]
    d1['cum_churned']      =   [ sum(d1.churned.values[0:i])+d1.iloc[i].churned for i in range(len(d1)) ]
    d1['cum_net']          =   [ sum(d1.net_change.values[0:i])+d1.iloc[i].net_change for i in range(len(d1)) ]
    d1['user_pool']        =   d1.cum_joined.values + d1.cum_churned.values
    d1['acqu_month']       =   [ row.joined/(row.user_pool)*100 for i, row in d1.iterrows() ]
    d1['churn_month']      =   [ row.churned/(row.user_pool)*100 for i, row in d1.iterrows() ]
    d1['net_change_pct']   =   d1.net_change / d1.user_pool*100
    d1['net_pct']          =   [ row.net_change/(row.user_pool)*100 for i, row in d1.iterrows() ]
    d1['cum_retention']    =   d1.cum_net/d1.cum_joined*100
    d1['cum_churn']        =   d1.cum_churned/d1.cum_joined*100
    d1['N_joined' ]        =   d1.joined
    d1['N_churned']        =   d1.churned

    if stringify:
        for colname in 	['acqu_month','churn_month','cum_retention','cum_churn','net_change_pct']:
            d1[colname] = d1[colname].apply(lambda x: stringify_pct(x))

    if all_columns:
        return d1

    return d1[['date','ownsCertifiedNFT','joined','churned','net_change',\
               'cum_joined','cum_churned','user_pool',\
               'acqu_month','churn_month','net_change_pct','cum_retention','cum_churn']]

@timer_func
def get_churn_tables(all_columns=False,
                     stringify=True, 
                     truncate_larger=True,
                     backshift_vars=[],
                     con = None):
    
    if con is None:
        con = get_alchemy_con()

    
    query = """
    select
        year(lastUsed) last_yr,
        month(lastUsed) last_mo,
        ownsCertifiedNFT,
  --      isWeb3,
        -count(user_id) churned

    from ( select * from fpa.users where (anonymous=0 or anonymous is null) and unclassifiedSignup=0) x

    group by 
        last_mo, last_yr, ownsCertifiedNFT, isWeb3

    having last_yr < 2023

    order by last_yr, last_mo
    ;
    """
    dd = pd.read_sql(query, con)
    dd['date'] = [ str(dd.last_yr.iloc[idx])+'-'+str(dd.last_mo.iloc[idx]) for idx, row in \
                   dd.iterrows() ]

    qj = """
    select
        year(created) c_yr,
        month(created) c_mo,
        ownsCertifiedNFT,
   --     isWeb3,
        count(user_id) joined

    from ( select * from fpa.users where (anonymous=0 or anonymous is null) and unclassifiedSignup=0) x

    group by 
        c_mo, c_yr, ownsCertifiedNFT

    having c_yr < 2023

    order by c_yr, c_mo

    ;"""
    
    dj = pd.read_sql(qj, con)
    dj['date'] = [ str(row.c_yr)+'-'+str(row.c_mo) for i, row in dj.iterrows() ]
    djd = dj[['date','joined','ownsCertifiedNFT']]
    ddd = dd[['date','churned', 'ownsCertifiedNFT']]
    df = djd.merge(ddd)[['date','joined','churned','ownsCertifiedNFT']]

    def stringify_pct(pct):
        if type(pct) == float:
            return '+'+str(round(pct,2))+'%' if pct>=0 else str(round(pct,2))+'%'
        else:
            return pct

    d1 = df[df.ownsCertifiedNFT==1]
    d0 = df[df.ownsCertifiedNFT==0]

    data_out = [d0,d1]
    output_transforms = [ 
                lambda x: transform_sql_response(x, all_columns=False, stringify=stringify),
                ]
        
    def apply_transforms(data,output_transforms=output_transforms):
        for tr in output_transforms:
            data = list(map(tr, data))
        return data
    return apply_transforms(data_out,output_transforms=output_transforms)

d00,d11 = get_churn_tables()

import numpy as np


def get_category(column):
    '''
    Returns the cateogory of the column to be plotted, useful for formatting the label for the legend.
    '''
    percentage_vars = [ 'acqu_month', 'churn_month','net_change_pct', 'cum_retention', 'cum_churn']
    if column not in percentage_vars:
        category = 'N'
    else:
        category = 'PCT'
    return category
        
        

def get_column_label(col):
    '''
    Returns the column label for the column provided as an argument, for the legend.
    '''
    start_char,end_char = '',''
    category = get_category(col)
    
    if category == 'N':
        end_char = ''
        start_char = 'N '
        
    elif category == 'PCT':
        end_char+=' (%)'
        start_char=''
        
    collabel = col
    if col.startswith('cum'):
        collabel = col.replace('cum_','cumulative_')
    collabel = collabel.replace('_',' ').strip()
    collabel = collabel.replace('pct','').strip()
    collabel = start_char + collabel 
    collabel += end_char
    return collabel



def plotify(kinds=None, 
            columns_to_barplot=None,
            VERBOSE=None,
            save=1):
    
    if kinds is None:
        kinds =  GlobalSettings.KINDS
        
    if VERBOSE is None:
        VERBOSE = GlobalSettings.VERBOSE
    

    def get_column_label(col):
        '''
        Returns the column label for the column provided as an argument, for the legend.
        '''
        start_char,end_char = '',''
        category = get_category(col)

        if category == 'N':
            end_char = ''
            start_char = 'N '

        elif category == 'PCT':
            end_char+=' (%)'
            start_char=''

        collabel = col
        if col.startswith('cum'):
            collabel = col.replace('cum_','cumulative_')
        collabel = collabel.replace('_',' ').strip()
        collabel = collabel.replace('pct','').strip()
        collabel = start_char + collabel 
        collabel += end_char
        return collabel

    def get_bar_shifts(nbars):
        x=nbars
        width = 1/x-1/2*1/x**2
        vec = []
        idx = -(x//2)
        while idx <= abs(x//2):
            vec.append(idx)
            idx+=1
        if x%2==0:
            vec.remove(0)
            return width, np.array(vec)/2
        return width, vec
    
    def barify_data(data: pd.DataFrame,
                    truncate=False)-> dict:
        '''
        Makes the data suitable for a barchart.
        '''
        data = data[['date']+list(data.columns[2:])]
        return { k: list(map(unstringify_pct, v.values())) if k!='date' else list(v.values()) for k, v in data.to_dict().items() }

    dataframes = {
        'usersWithCertifiedNFT': d11,
        'usersWithoutCertifiedNFT': d00
        }

    bardata = { k: barify_data(v, truncate=1) for k,v in dataframes.items() }

    if columns_to_barplot is None:
        columns_to_barplot = [
        'cum_retention', 'cum_churn', 'net_change_pct'
        ]
   

    for kind in kinds:
        for col in columns_to_barplot:
            print(bardata.keys())
            VERBOSE and print(f'bardata[{kind}][{col}]:', bardata[kind][col])
        
        labels = bardata[kind]['date']
        fig, ax = make_big(f=22,w=30,h=17)
        pos = np.arange(len(labels))
        
        ax.set_xticks(pos)
        ax.set_xticklabels(labels)

        kinds = GlobalSettings.KINDS
        bar_ax_objects = { kind: dict() for kind in kinds }

        bar_width, bar_shifts = get_bar_shifts(len(columns_to_barplot))
        bar_width = round(bar_width,1)


        for i, col in enumerate(columns_to_barplot):
            barY   = bardata[kind][col]
            shift = bar_shifts[i]
            collabel = get_column_label(col)
            ff = f'''bar_ax_objects[{kind}][{col}] = ax.bar(pos+{bar_width}*{shift}, barY, {bar_width}, label=f'{collabel}')'''
            VERBOSE and print(ff)
            cols = ','.join(columns_to_barplot)
            title = f'''{kind}'''
            ax.set_title(title)

            axis_obj = ax.bar(pos+bar_width*shift, list(map(lambda x: round(x, 1), barY)) , bar_width,label=f'{collabel}')
            bar_ax_objects[kind][col] = axis_obj
            ax.bar_label(axis_obj)
            # plt.ylabel('%')
        plt.xticks(rotation=70)
        plt.legend()       
        if save:
            plt.savefig(f'/Users/allen/Desktop/barplots/'+f'{GlobalSettings.get_plotno()}_'+f'{kind}'+'_'.join(columns_to_barplot)+'.jpg')
        
        plt.show()
        
#         if save:
#             plt.savefig('/Users/allen/Desktop/barplots/'+'_'.join(columns_to_barplot)+'.jpg')
        
        

def main():


    kinds = GlobalSettings.KINDS

                                #'cum_churn'])
    plotify(
            kinds=kinds, 
            columns_to_barplot=[
                                'acqu_month',
                                'churn_month', 
                                'net_change_pct',
                                # 'cum_retention',
                                # 'cum_churn',
                                # 'joined',
                                # 'churned',
                                # 'net_change',
                                # 'user_pool'#])
                               ])
    plotify(
            kinds=kinds, 
            columns_to_barplot=[
                                # 'acqu_month',
                                # 'churn_month', 
                                'net_change_pct',
                                'cum_retention',
                                'cum_churn',
                                # 'joined',
                                # 'churned',
                                # 'net_change',
                                # 'user_pool'#])
                               ])


    plotify(
            kinds=kinds, 
            columns_to_barplot=[
                                'acqu_month',
                                'churn_month', 
                                'net_change_pct',
                                'cum_retention',
                                'cum_churn',
                                # 'joined',
                                # 'churned',
                                # 'net_change',
                                # 'user_pool'#])
                               ])

    plotify(
            kinds=kinds, 
            columns_to_barplot=[
                                # 'acqu_month',
                                # 'churn_month', 
                                # 'net_change_pct',
                                # 'cum_retention',
                                # 'cum_churn',
                                'joined',
                                'churned',
                                'net_change',
                                'user_pool'#])
                               ])


    plotify(
            kinds=kinds, 
            columns_to_barplot=[
                                # 'acqu_month',
                                # 'churn_month', 
                                # 'net_change_pct',
                                # 'cum_retention',
                                # 'cum_churn',
                                'cum_joined',
                                'cum_churned',
                                'user_pool'#])
                               ])
if __name__ == '__main__':
    main()
    