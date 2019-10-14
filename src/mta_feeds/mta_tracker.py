import csv
from collections import defaultdict
import json

from datetime import datetime
from confluent_kafka import Consumer, KafkaError

import pandas as pd 
import argparse

from mta_feeds import (melt_arrival_df, 
                       create_additional_columns, 
                       create_sql_tables,
                       insert_data_files,
                       merge_stops)


parser = argparse.ArgumentParser(description='Get realtime info for feed')
parser.add_argument('feed', type=int, default=1, metavar='feed', help='the feed which you would like to pull from')
args = parser.parse_args() 


class MTATrainTracker(object):

    def __init__(self, feed):
        self.kafka_consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test_consumer_group',
            'default.topic.config': {
                'auto.offset.reset': 'smallest'
            }
        })
        self.kafka_topic = str(feed)

        # subway line number -> (stop_id, direction) -> next arrival time
        self.arrival_times = defaultdict(lambda: defaultdict(lambda: -1))

        self.stations = {}
        with open('../../data/mta_info/Stations.csv') as csvf:
            reader = csv.DictReader(csvf)
            for row in reader:
                self.stations[row['GTFS Stop ID']] = row['Stop Name']

    def process_message(self, message):
        now = pd.to_datetime('now', utc=True).tz_convert('US/Eastern')
        trip_update = json.loads(message)

        trip_header = trip_update.get('trip')
        if not trip_header:
            return

        route_id = trip_header['routeId']
        stop_time_updates = trip_update.get('stopTimeUpdate')
        if not stop_time_updates:
            return

        trip_id = trip_header['tripId'] 
        data_list = []
        for update in stop_time_updates:
            if 'arrival' not in update or 'stopId' not in update:
                continue
            
            stop_id = update['stopId']
            new_arrival_ts = pd.to_datetime(int(update['arrival']['time']), unit='s', utc=True).tz_convert('US/Eastern')

            
            difference = (new_arrival_ts - now).total_seconds()/60

            update_dict = {'stop_id': stop_id,
                           'route_id': route_id,
                           'trip_id': trip_id,
                           'arrival_est': new_arrival_ts,
                           'current_time': now,
                           'estimated_TTA': difference,
                           'between_stops': difference > 0, 
                           'time_at_station': abs(difference) if difference <= 0 else None}

            return update_dict


    def restructure_df(self, df):
        map_cols = {'trip_id':'trip',
                    'stop_id': 'stop',
                    'arrival_est':'arrival',
                    'route_id': 'train',
                    'depart': 'departure'}
        df.rename(columns=map_cols, inplace=True)

        df['departure'] = df['arrival'] + pd.to_timedelta(df['time_at_station'],        unit='s')
        
        other_cols = [col for col in df.columns if col not in map_cols.values()]
        df.drop(columns=other_cols, inplace=True)

    
        return df

    def manipulate_csv(self):
        df = self.update_df
        df['last_stop'] = df.groupby(['trip_id'])['stop_id'].transform('last')
        df['arrival_est'] = pd.to_datetime(df['arrival_est']).dt.tz_convert('US/Eastern')
        now = pd.to_datetime('now', utc=True).tz_convert('US/Eastern')
        mask = (df['stop_id'] != df['last_stop']) | ((now - df['arrival_est']).dt.total_seconds() > 30 * 5)


        
        subset = df[mask].groupby(['trip_id', 'stop_id']).last().reset_index().copy()
        self.update_df = df[~mask].reset_index(drop=True)
        subset = self.restructure_df(subset)
        subset = melt_arrival_df(subset).sort_values(by=['trip', 'time']).reset_index(drop=True)
        if not subset.empty:
            subset = merge_stops(subset)
            subset = create_additional_columns(subset)
            t, s, tr = create_sql_tables(subset)
            insert_data_files(t, s, tr)

    def run(self):
        self.kafka_consumer.subscribe([self.kafka_topic])


        save = False
        update_list = []
        self.update_df = pd.DataFrame(columns=['stop_id', 'route_id', 'trip_id',
                                          'direction', 'arrival_est','current_time',
                                          'estimated_TTA', 'between_stops', 'time_at_station'])
        while True:
            msg = self.kafka_consumer.poll(1.0)

            if msg is None or not msg.value():
                if save:
                    self.manipulate_csv()
                    self.update_df.to_csv('test.csv')
                    save = False
                continue
            if msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                raise ValueError('Kafka consumer exception: {}'.format(msg.error()))

            msg = msg.value()
            update_dict = self.process_message(msg.decode('utf-8'))
            self.update_df = self.update_df.append(update_dict, ignore_index=True)
            save = True

if __name__=='__main__':
    feed = args.feed
    tracker = MTATrainTracker(feed)
    tracker.run()