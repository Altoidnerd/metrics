import datetime
import pandas as pd
import os
import glob
import warnings

def main():
    warnings.filterwarnings("ignore")

    #os.chdir('''/Users/allen/Desktop/clo/flickplay/mixpanel-events/data''')
    os.chdir('''./data''')
    infiles = list(sorted(glob.glob('*.csv')))
    bigdf=pd.DataFrame(columns = [ c for c in pd.read_csv(infiles[0]).columns if c not in ['Unnamed: 0'] ])

    for infile in infiles[:20]:
        df = pd.read_csv(infile, low_memory=True)
        df.index=df.date
        df = df.drop(columns = ['Unnamed: 0'])
        print(min(df.index),max(df.index))
        bigdf = pd.concat([bigdf, df])
        print(len(df), len(bigdf))

    # bigdf.index = [ datetime.date.fromtimestamp(ts).isoformat() for ts in bigdf.time]
    # bigdf['date'] = bigdf.index.values
    bigdf['date'] = [ datetime.date.fromtimestamp(ts).isoformat() for ts in bigdf.time]
    bigdf.to_csv('bigdf_out.csv', index=False)
    
    print(f'Created from infiles: {'\n'.join(infiles)})
          
    return 0
