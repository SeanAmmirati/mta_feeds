from mta_feeds import dbconnector
import datetime
import pandas as pd
import numpy as np
import re
import urllib

import yaml




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

def read_turnstile_csv(filename, func=pd.read_csv, counter=0):

    if counter == 7:
        print(f'Attempted a full week sweep. Check this filename: {filename}. Returning None for this file.')
        return None
    try:
        df = func(filename)
    except urllib.error.HTTPError:
        new_fn = move_filename_up_a_day(filename)
        df = read_turnstile_csv(new_fn, func=func, counter=counter + 1)
    return df


def pre_oct_2014_fileimport(filename, chunksize=1000):
    frmt = '%y%m%d'
    colspec = [x.strip() for x in
                '''C/A,UNIT,SCP,
                DATE1,TIME1,DESC1,ENTRIES1,EXITS1,
                DATE2,TIME2,DESC2,ENTRIES2,EXITS2,
                DATE3,TIME3,DESC3,ENTRIES3,EXITS3,
                DATE4,TIME4,DESC4,ENTRIES4,EXITS4,
                DATE5,TIME5,DESC5,ENTRIES5,EXITS5,
                DATE6,TIME6,DESC6,ENTRIES6,EXITS6,
                DATE7,TIME7,DESC7,ENTRIES7,EXITS7,
                DATE8,TIME8,DESC8,ENTRIES8,EXITS8'''.replace('\n', '').split(',')]


    df = pd.read_csv(filename, names=colspec, header=None)


    df['idx'] = df.index
    df = pd.wide_to_long(df, stubnames=['DATE', 'TIME', 'DESC', 'ENTRIES', 'EXITS'],
                         i=['idx'], j='temp').reset_index(drop=True)
    df.dropna(inplace=True, how='any')

    with open('../../data/ca_to_station.yml', 'r') as f:
        ca_to_station = yaml.load(f)

    with open('../../data/ca_to_linenames.yml', 'r') as f:
        ca_to_linenames = yaml.load(f)

    with open('../../data/ca_to_division.yml', 'r') as f:
        ca_to_division = yaml.load(f)

    df['STATION'] = df['C/A'].map(ca_to_station)
    df['LINENAME'] = df['C/A'].map(ca_to_linenames)
    df['DIVISION'] = df['C/A'].map(ca_to_division)
    df['RECORD_DATE'] = pd.to_datetime(df['DATE']) + pd.to_timedelta(df['TIME'])
    df.drop(columns=['DATE', 'TIME'], inplace=True)


    # df.drop(columns=['temp', 'idx'], inplace=True)
    df.dropna(subset=['STATION', 'LINENAME', 'DIVISION'], inplace=True)
    return df

def post_oct_2014_fileimport(filename):

    df = pd.read_csv(filename)
    df.columns = [x.strip() for x in df.columns]
    df['RECORD_DATE'] = pd.to_datetime(df['DATE']) + pd.to_timedelta(df['TIME'])
    df.drop(columns=['DATE', 'TIME'], inplace=True)
    return df

def read_csvs_in_range(f='05-01-2010', t=None):
    with open('../../data/turn_to_stops.yml', 'r') as fl:
        stop_map = yaml.load(fl)

    df_list = []
    fns = generate_filenames(f, t)
    for fn, dt in fns.items():
        func = pre_oct_2014_fileimport if dt < pd.to_datetime('10-18-2014') else post_oct_2014_fileimport
        df = read_turnstile_csv(fn, func=func)
        df['STATION'] = df['STATION'].map(stop_map)
        df = df[~df['DIVISION'].isin(['RIT', 'PTH'])].reset_index(drop=True)
        df_list.append(df)
    return pd.concat(df_list)

def send_to_sql_in_range(f='05-01-2010', t=None, conn=None):
    if conn is None:
        conn = dbconnector()
    with open('../../data/turn_to_stops.yml', 'r') as fl:
        stop_map = yaml.load(fl)

    changed = {'F': {'Lexington': 'Lexington Av/63 St'},
               '1': {'Cathedral': 'Cathedral Pkwy'}}

    stops = pd.read_sql('SELECT DISTINCT stationid, daytimeroutes, stopname FROM station;', conn)
    stops['match'] = stops['stopname'].str.upper() + '/' + stops['daytimeroutes'].str.replace(' ', '').str.upper()
    stops_to_id = stops.set_index('match').to_dict()['stationid']

    df_list = []
    fns = generate_filenames(f, t)
    for fn, dt in fns.items():
        print(fn)
        print(dt)
        func = pre_oct_2014_fileimport if dt < pd.to_datetime('10-18-2014') else post_oct_2014_fileimport
        df = read_turnstile_csv(fn, func=func)
        df = df[~df['DIVISION'].isin(['RIT', 'PTH'])].reset_index(drop=True)

        import pdb; pdb.set_trace()
        df['STATION'] = df['STATION'].map(stop_map).str.upper()
        df['STATION'] = df.apply(lambda x: specific_changed(x, changed), axis=1)


        df.loc[df['STATION'].isin(['ST GEORGE', 'TOMPKINSVILLE']), 'LINENAME'] = 'SIR'

        df['regex_str'] = df['STATION'].astype(str).apply(lambda x: '(^|[\D])' + re.escape(x)) + '.*/[' + df['LINENAME'] + ']+'
        df['id'] = df['regex_str'].map(stops_to_id)
        regex_to_stopid = df.groupby('regex_str').apply(lambda x: find_correct_station(stops, x.name)).to_dict()
        df['stationid'] = df['regex_str'].map(regex_to_stopid)
        import pdb; pdb.set_trace()
        return df
        # df.to_sql('turnstile', con=conn, if_exists='drop')
def find_correct_station(stops, regex_string):
    all_possible = stops['match'].str.contains(regex_string)
    if all_possible.sum() == 0:
        return None 
    else:
        return stops.loc[all_possible.fillna(False).idxmax(), 'stationid']

def specific_changed(series, changed):
    if series['LINENAME'] in changed:
        for k, v in changed[series['LINENAME']].items():
            if k.upper() in series['STATION']:
                return v.upper()
    return series['STATION']
    
if __name__ == '__main__':
    conn = dbconnector()
    # df = read_csvs_in_range(f='10-10-2010', t='10-25-2010')
    df = send_to_sql_in_range(f='10-01-2019', t='10-25-2019')
    import pdb; pdb.set_trace()