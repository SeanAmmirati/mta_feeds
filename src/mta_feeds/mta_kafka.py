import time

import os
import requests
from google.protobuf.json_format import MessageToJson
from confluent_kafka import Producer

from google.transit import gtfs_realtime_pb2
import argparse

import json


parser = argparse.ArgumentParser(description='Get realtime info for feed')
parser.add_argument('feed', type=int, metavar='feed', help='the feed which you would like to pull from')
args = parser.parse_args() 


class MTARealTime(object):

    def __init__(self, feed):

        self.api_key = os.environ['MTA_API_KEY']

        self.mta_api_url = 'http://datamine.mta.info/mta_esi.php?key={}&feed_id='.format(
            self.api_key)
        self.kafka_topic = str(feed)
        self.kafka_producer = Producer({'bootstrap.servers': 'localhost:9092'})

    def produce_trip_updates(self):
        feeds = [1, 26, 16, 21, 2, 11, 31, 36, 51]
        for fd in feeds: 
            url = self.mta_api_url + str(fd)

            feed = gtfs_realtime_pb2.FeedMessage()
            response = requests.get(url)
            feed.ParseFromString(response.content)

            for entity in feed.entity:
                if entity.HasField('trip_update'):
                    update_json = MessageToJson(entity.trip_update)
  
                self.kafka_producer.produce(
                    self.kafka_topic, update_json.encode('utf-8'))

                self.kafka_producer.flush()

    def run(self):
        while True:
            print('looped')
            self.produce_trip_updates()
            time.sleep(30)



if __name__ == '__main__':
    feed = args.feed
    mta = MTARealTime(feed)
    mta.run()
