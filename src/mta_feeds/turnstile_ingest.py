from mta_feeds import dbconnector
import datetime
import pandas as pd
import numpy as np
import re
import urllib




def generate_filenames(f='05-01-2010', t=None):
    from_dt = pd.to_datetime(f)
    to_dt = pd.to_datetime(t) if t is not None else datetime.datetime.now()

    bump_ahead = (12 - from_dt.weekday()) % 7

    dt = from_dt + pd.to_timedelta(bump_ahead, unit='days')

    week_timedelta = pd.to_timedelta(7, unit='days')

    filenames = {}

    link_base = 'http://web.mta.info/developers/data/nyct/turnstile/turnstile_{}.txt'
    frmt = '%y%m%d'

    while dt < to_dt:

        formatted_time = dt.strftime(frmt)
        frmt_string = link_base.format(formatted_time)
        filenames[frmt_string] = dt

        dt += week_timedelta

    return filenames

def move_filename_up_a_day(filename):
    frmt = '%y%m%d'
    dt_str = re.findall('\d{6}', filename)[0]
    dt = pd.to_datetime(dt_str, format=frmt)
    dt += pd.to_timedelta(1, unit='d')
    return filename.replace(dt_str, dt.strftime(frmt))

def read_turnstile_csv(filename, func=pd.read_csv, counter=0, conn=None):

    if counter == 7:
        print(f'Attempted a full week sweep. Check this filename: {filename}. Returning None for this file.')
        return None
    try:
        df = func(filename, conn=conn)
    except urllib.error.HTTPError:
        new_fn = move_filename_up_a_day(filename)
        df = read_turnstile_csv(new_fn, func=func, counter=counter + 1, conn=conn)
    return df


def pre_oct_2014_fileimport(filename, chunksize=1000, conn=None):
    colspec = '''C/A,UNIT,SCP,
                DATE1,TIME1,DESC1,ENTRIES1,EXITS1,
                DATE2,TIME2,DESC2,ENTRIES2,EXITS2,
                DATE3,TIME3,DESC3,ENTRIES3,EXITS3,
                DATE4,TIME4,DESC4,ENTRIES4,EXITS4,
                DATE5,TIME5,DESC5,ENTRIES5,EXITS5,
                DATE6,TIME6,DESC6,ENTRIES6,EXITS6,
                DATE7,TIME7,DESC7,ENTRIES7,EXITS7,
                DATE8,TIME8,DESC8,ENTRIES8,EXITS8'''.replace('\n', '').strip().split(',')


    df = pd.read_csv(filename, names=colspec, header=None)

    for i in range(0, df.shape[0], chunksize):
        d = df.iloc[0:chunksize]
        new_d = pd.wide_to_long(d, stubnames=['DATE', 'TIME', 'DESC', 'ENTRIES', 'EXITS'],
                         i=['C/A', 'UNIT', 'SCP'], j='temp')
        df = df.iloc[chunksize:]
        import pdb; pdb.set_trace()
        new_d.to_sql('turnstile_info', conn, if_exists='append')


    df = pd.concat(df_list)
    df.drop(columns='temp', inplace=True)
    return df

def post_oct_2014_fileimport(filename, conn=None):
    return pd.read_csv(filename)

def read_csvs_in_range(f='05-01-2010', t=None, conn=None):
    df_list = []
    fns = generate_filenames(f, t)
    print(fns)
    for fn, dt in fns.items():
        func = pre_oct_2014_fileimport if dt < pd.to_datetime('10-18-2014') else post_oct_2014_fileimport
        df = read_turnstile_csv(fn, func=func, conn=conn)
        df_list.append(df)
    return pd.concat(df_list)



if __name__ == '__main__':
    conn = dbconnector()
    df = read_csvs_in_range(f='05-01-2010', t='05-10-2010',conn=conn)
    import pdb; pdb.set_trace()
