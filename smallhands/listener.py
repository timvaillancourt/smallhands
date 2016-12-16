import json
import logging
import pymongo
import sys

from datetime import timedelta
from dateutil import parser
from random import randint
from time import sleep, time
from tweepy.streaming import StreamListener


class SmallhandsListener(StreamListener):
    def __init__(self, db, config, count=0):
        self.db     = db
        self.config = config
        self.count  = count
        self.logger = logging.getLogger(__name__)

        self.last_report_time  = int(time())
        self.last_report_count = self.count

    def parse_tweet(self, data):
        try:
            parsed = data
            if isinstance(data, dict):
                parsed = {}
                for key in data:
                    if key == "created_at":
                        data[key] = parser.parse(data[key])
                    parsed[key] = self.parse_tweet(data[key])
            elif isinstance(data, list):
                parsed = []
                for item in data:
                    parsed.append(self.parse_tweet(item))
            return parsed
        except Exception, e:
            self.logger.error("Error parsing tweet: %s" % e)

    def process_tweet(self, data):
        try:
            tweet = self.parse_tweet(json.loads(data))
            if not 'id' in tweet:
                self.logger.warn("No 'id' field in tweet data, skipping")
                return None
            if 'created_at' in tweet and 'expire' in self.config.db:
                if 'min_secs' in self.config.db.expire and 'max_secs' in self.config.db.expire:
                    ttl_secs = randint(self.config.db.expire.min_secs, self.config.db.expire.max_secs)
                    tweet['expire_at'] = tweet['created_at'] + timedelta(seconds=ttl_secs)
            return tweet
        except Exception, e:
            self.logger.error("Error processing tweet: %s" % e)

    def on_data(self, data):
        tweet = self.process_tweet(data)
        now = time()
        if int(now) >= int(self.last_report_time) + self.config.report_interval:
            count = self.count - self.last_report_count
            tps = float(count) / float(self.config.report_interval)
            self.logger.info("Processed %i tweets (total: %i) in %i seconds (%f per sec)" % (count, self.count, self.config.report_interval, tps))
            self.last_report_count = self.count
            self.last_report_time  = now
        try:
            if tweet:
                self.db['tweets'].insert(tweet)
                if 'user' in tweet:
                    if 'expire_at' in tweet:
                        tweet['user']['expire_at'] = tweet['expire_at']
                    self.db['users'].update_one({ 'id': tweet['user']['id'] }, { '$set' : tweet['user'] }, upsert=True)
                self.count += 1
        except pymongo.errors.DuplicateKeyError, e:
            pass
        except Exception, e:
            self.logger.error("Error writing tweet to db: %s" % e)

    def on_error(self, e):
        msg = e
        if e == 401:
            msg = "401 Unauthorized: Authentication credentials were missing or incorrect"
        elif e == 420:
            msg = "420 Rate limiting"
            return sleep(3)
        elif e == 429:
            msg = "429 Too Many Requests"
            return sleep(5)
        self.logger.error("Twitter Streaming API error: '%s'" % msg)
