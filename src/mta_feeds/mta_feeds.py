import requests
import os 
from google.transit import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict  
from zipfile import ZipFile
from io import BytesIO

import pandas as pd 


API_KEY = os.environ['MTA_API_KEY']
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


def run_through_all_trains():
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
        df[time_col] = pd.to_datetime(df[time_col], unit='s', utc=True).dt.tz_convert('EST')

    df = df[df['train'].notnull()].reset_index()
    return df

def melt_arrival_df(df):
    return df.melt(id_vars=['trip', 'stop', 'train'], 
                    var_name='type', 
                    value_name='time',
                    value_vars=['arrival', 'departure'])


if __name__ == '__main__':
    feed = feed_message()
    d = convert_to_dict(feed)
    df = create_arrival_df(d)
    melted = df.melt(id_vars=['trip', 'stop', 'train'], 
                     var_name='type', 
                     value_vars=['arrival', 'departure'])

