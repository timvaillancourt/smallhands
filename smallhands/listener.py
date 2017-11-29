import json
import logging
import pymongo

from datetime import timedelta
from dateutil import parser
from random import randint
from time import sleep, time
from tweepy.streaming import StreamListener


class Listener(StreamListener):
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
        except Exception as e:
            self.logger.error("Error parsing tweet: %s" % e)

    def process_tweet(self, data):
        try:
            tweet = self.parse_tweet(json.loads(data))
            if 'id' not in tweet:
                return None
            if 'created_at' in tweet and 'expire' in self.config.db:
                if 'min_secs' in self.config.db.expire and 'max_secs' in self.config.db.expire:
                    ttl_secs = randint(self.config.db.expire.min_secs, self.config.db.expire.max_secs)
                    tweet['expire_at'] = tweet['created_at'] + timedelta(seconds=ttl_secs)
            return tweet
        except Exception as e:
            self.logger.error("Error processing tweet: %s" % e)

    def on_data(self, data):
        now   = time()
        tweet = self.process_tweet(data)
        if int(now) >= int(self.last_report_time) + self.config.report_interval:
            count = self.count - self.last_report_count
            tps = float(count) / float(self.config.report_interval)
            self.logger.info("Processed %i tweets (%f per sec, stream total: %i) in the last %i seconds" % (count, tps, self.count, self.config.report_interval))
            self.last_report_count = self.count
            self.last_report_time  = now
        try:
            if tweet:
                self.db['tweets'].insert(tweet)
                if 'user' in tweet:
                    if 'expire_at' in tweet:
                        tweet['user']['expire_at'] = tweet['expire_at']
                    self.db['users'].update_one(
                        {'id': tweet['user']['id']},
                        {'$set': tweet['user']},
                        upsert=True
                    )
                self.count += 1
        except pymongo.errors.DuplicateKeyError as e:
            pass
        except Exception as e:
            self.logger.error("Error writing tweet to db: %s" % e)

    def on_error(self, e):
        msg = e
        errors = {
            401: "401 Unauthorized: Authentication credentials were missing or incorrect",
            420: "420 Rate limiting",
            429: "429 Too Many Requests"
        }
        if e in errors:
            msg = errors[e]
        self.logger.error("Twitter Streaming API error: '%s'" % msg)
        if e >= 420:
            sleep(3)
