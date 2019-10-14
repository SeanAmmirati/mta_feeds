import requests
import os 
from google.transit import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict  
from zipfile import ZipFile
from io import BytesIO

import pandas as pd 
import numpy as np

from datetime import datetime
from pytz import timezone

from sqlalchemy import create_engine


API_KEY = os.environ['MTA_API_KEY']
DB_UN = os.environ['DB_UN']
DB_PW = os.environ['DB_PW']
DB_LOC = os.environ['DB_LOC']

db_string = f'postgres://{DB_UN}:{DB_PW}@{DB_LOC}'
req_str = f'http://datamine.mta.info/mta_esi.php?key={API_KEY}'

feed_ids = [1, 26, 16, 21, 2, 11, 31, 36, 51]


def merge_stops(df, data_dir='../../data/mta_info'):
    if 'stops.txt' not in os.listdir(data_dir):
        download_rideinf()

    full_path  = os.path.join(data_dir, 'stops.txt')
    temp_df = pd.read_csv(full_path)

    new_df = df.merge(temp_df, how='left', left_on='stop', right_on='stop_id')

    del temp_df 
    return new_df

    

def download_rideinf():
    mta_specs_link =  'http://web.mta.info/developers/data/nyct/subway/google_transit.zip'
    temp_r = requests.get(mta_specs_link, allow_redirects=True)
    with ZipFile(BytesIO(temp_r.content)) as zipped:
        zipped.extractall('../../data/mta_info') 

def generate_dataframe(feed_ids=[1, 26, 16, 21, 2, 11, 31, 36, 51], 
                       data_dir='../../data/mta_info', 
                       req_str=req_str):
    df = run_through_trains(feed_ids)
    df = merge_stops(df, data_dir)
    return df



def run_through_trains(feed_ids=[1, 26, 16, 21, 2, 11, 31, 36, 51]):
    df_list = []
    for feed in feed_ids:
        fd_st = str(feed)
        req_str_feed = f'{req_str}&feed_id={fd_st}'
        feed = feed_message(req_str_feed)
        d = convert_to_dict(feed)
        df = create_arrival_df(d)
        df_list.append(df)
    full_df = pd.concat(df_list)
    full_df = melt_arrival_df(full_df).sort_values(by=['trip', 'time']).reset_index(drop=True)
    full_df = create_additional_columns(full_df)
    return full_df
    
def create_additional_columns(df):
    df['direction'] = df['stop'].str[-1].map({'N': 'North', 'S': 'South'})
    df['stop_number'] = df['stop'].str[1:-1].astype(int)
    df['on_line'] = df['stop'].str[0]
    return df
    
def feed_message(req_str):
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(req_str)
    feed.ParseFromString(response.content)
    return feed

def convert_to_dict(feed):
    fd = protobuf_to_dict(feed)
    realtime = fd['entity']
    return realtime


def create_arrival_df(l_of_updates):
    i = 1
    trip_id_to_len = {}

    for update in l_of_updates:
        if 'trip_update' in update:
            trip_id = update['trip_update']['trip']['trip_id']
            present_in_dict = trip_id in trip_id_to_len
            not_already_added = 'stops' not in trip_id_to_len[trip_id] if present_in_dict else True
            if not_already_added:
                if not present_in_dict:
                    if 'stop_time_update' in update['trip_update']:
                        updates = update['trip_update']['stop_time_update']
                    else:
                        print(update['trip_update'].keys())
                        continue
                    trip_id_to_len[trip_id] = {'stops': updates}
                else:
                    trip_id_to_len[trip_id].update({'stops': updates})

        elif 'vehicle' in update:
            trip_id = update['vehicle']['trip']['trip_id']
            train = update['vehicle']['trip']['route_id']
            if update['vehicle']['trip']['trip_id'] in trip_id_to_len:
                trip_id_to_len[trip_id].update({'train': train})
            else:
                trip_id_to_len[trip_id] = {'train': train}


    data = []
    for trip, value in trip_id_to_len.items():
        if 'stops' in value:
            for val in value['stops']:
                entry = {}
                entry['trip'] = trip
                entry['stop'] = val['stop_id']
                entry['arrival'] = val['arrival']['time'] if 'arrival' in val else None
                entry['departure'] = val['departure']['time'] if 'departure' in val else None
                entry['train'] = value['train'] if 'train' in value else None
                data.append(entry)    
    df = pd.DataFrame(data)

    for time_col in ['arrival', 'departure']:
        df[time_col] = pd.to_datetime(df[time_col], unit='s', utc=True).dt.tz_convert('America/New_York')

    df = df[df['train'].notnull()].reset_index()
    return df

def melt_arrival_df(df):
    melted = df.melt(id_vars=['trip', 'stop', 'train'], 
                    var_name='type', 
                    value_name='time',
                    value_vars=['arrival', 'departure'])
    melted = melted[melted['time'] <= datetime.now(tz=timezone('America/New_York'))].reset_index()
    print(melted)
    return melted

def create_sql_tables(df, trip_start_idx=0, engine=None):

    
    trip_df = df[['trip', 'direction', 'train']]
    trip_df = trip_df.drop_duplicates().reset_index(drop=True)
    start_idx = trip_start_idx
    map_trips = (trip_df[['trip']].reset_index().set_index('trip') + start_idx).to_dict()['index']

    trip_df['id'] = trip_df['trip'].map(map_trips)
    df['trip'] = df['trip'].map(map_trips)

    stop_df = df[['stop', 'stop_number', 'on_line', 'stop_code', 
                  'stop_name', 'stop_lat', 'stop_lon', 'zone_id', 
                  'stop_url', 'location_type', 'parent_station']]

    stop_df = stop_df.drop_duplicates().reset_index(drop=True)

    times_df = df[['trip', 'stop', 'type', 'time']]

    return times_df, stop_df, trip_df

def dbconnector():
    return create_engine(db_string)

def reset_database():
    db = dbconnector()
    db.execute('DELETE FROM stops;')
    db.execute('DELETE FROM trip;')
    db.execute('DELETE FROM actions;')

def filter_added_stops(stop_df, engine):
    current_stops = pd.read_sql('SELECT * FROM stops;', con=engine)

    stop_df.rename(columns={'stop': 'id'}, inplace=True)

    return only_nonduplicates(current_stops, stop_df, 'id')

def only_nonduplicates(df1, df2, subset=None):
    if df1.empty:
        return df2
    try:
        df2 = df2.astype(df1.dtypes.to_dict())
    except:
        import pdb; pdb.set_trace()
    df_all = df2.merge(df1, on=subset, how='left', indicator=True)
    df_filtered = df_all[df_all['_merge'] == 'left_only'].drop(columns='_merge')
    return df2.loc[df_filtered.index]

def filter_added_trips(trip_df, times_df, engine):
    in_query = str(tuple(trip_df['trip'].tolist())) if len(trip_df['trip'].tolist()) > 1 else f'({trip_df["trip"].tolist(0)})'



    current_trips = pd.read_sql(f'SELECT * FROM trip WHERE trip IN {in_query}', con=engine)
    map_to_idx = trip_df[['trip']].merge(current_trips[['trip', 'id']]).set_index('trip').to_dict()['id']
    change_bool = trip_df['trip'].isin(current_trips['trip'])
    max_fetch = engine.execute('SELECT MAX(id) FROM trip;').fetchall()
    max_trip_id = max_fetch[0][0] + 1 if max_fetch[0][0] is not None else 0

    trip_df.loc[change_bool, 'new_id'] = trip_df.loc[change_bool, 'trip'].map(map_to_idx)
    trip_df.loc[~change_bool, 'new_id'] = np.arange(0, (~change_bool).sum()) + max_trip_id
    old_id_to_new = trip_df[['new_id', 'id']].set_index('id').to_dict()['new_id']

    times_df['trip'] = times_df['trip'].map(old_id_to_new)
    trip_df['id'] = trip_df['new_id']
    trip_df.drop(columns='new_id', inplace=True)
    return times_df, only_nonduplicates(current_trips, trip_df)

def filter_added_actions(times_df, engine):
    min_time = str(times_df['time'].min())
    current_actions = pd.read_sql(f"SELECT * FROM actions WHERE time >= '{min_time}'", con=engine)
    return only_nonduplicates(current_actions.drop(columns=['id']), times_df)

def insert_data_files(times_df, stop_df, trip_df, engine=None):

    engine = dbconnector() if engine is None else engine

    stop_df = filter_added_stops(stop_df, engine)
    times_df, trip_df = filter_added_trips(trip_df, times_df, engine)
    times_df = filter_added_actions(times_df, engine).dropna(how='all')

    stop_df.to_sql('stops', index=False, con=engine, if_exists='append')
    trip_df.to_sql('trip', index=False, con=engine, if_exists='append')
    times_df.to_sql('actions', con=engine, index=False, if_exists='append')
    

if __name__ == '__main__':
    data_dir = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../data/mta_info'))
    df = generate_dataframe(data_dir=data_dir)
    tbls = create_sql_tables(df)
    insert_data_files(*tbls)
    print('Data ingested successfully!')
