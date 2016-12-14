#!/usr/bin/env python

from datetime import timedelta
from dateutil import parser
from random import randint
from time import sleep
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
from yconf import BaseConfiguration

import json
import logging
import pymongo
import signal
import sys

__VERSION__ = "0.4"
    

class SmallhandsListener(StreamListener):
    def __init__(self, db, config):
        self.db     = db
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.count  = 0

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
            if 'created_at' in tweet and 'expire' in self.config.db:
                if 'min_secs' in self.config.db.expire and 'max_secs' in self.config.db.expire:
                    ttl_secs = randint(self.config.db.expire.min_secs, self.config.db.expire.max_secs)
                    tweet['expire_at'] = tweet['created_at'] + timedelta(seconds=ttl_secs)
            return tweet
        except Exception, e:
            self.logger.error("Error processing tweet: %s" % e)

    def on_data(self, data):
        tweet = self.process_tweet(data)
        try:
            self.db['tweets'].insert(tweet)
            if 'user' in tweet:
                if 'expire_at' in tweet:
                    tweet['user']['expire_at'] = tweet['expire_at']
                self.db['users'].update_one({ 'id': tweet['user']['id'] }, { '$set' : tweet['user'] }, upsert=True)
            self.count += 1
            if (self.count % 50) == 0:
                self.logger.info("Wrote 50 tweets (total: %i)" % self.count)
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
        self.logger.error("Twitter Streaming API error: '%s'" % msg, True)


class SmallhandsConfig(BaseConfiguration):
    def makeParser(self):
        parser = super(SmallhandsConfig, self).makeParser()
        parser.add_argument("-H", "--db-host", dest="db.host", help="MongoDB hostname/ip")
        parser.add_argument("-P", "--db-port", dest="db.port", type=int, help="MongoDB TCP port")
        parser.add_argument("-D", "--db-name", dest="db.name", help="MongoDB Database name (default: smallhands)")
        parser.add_argument("-u", "--db-user", dest="db.user", help="(Optional) MongoDB Username")
        parser.add_argument("-p", "--db-password", dest="db.password", help="(Optional) MongoDB Password")
        parser.add_argument("-d", "--db-authdb", dest="db.authdb", help="MongoDB Authentication Database (default: admin)")
        parser.add_argument("-x", "--db-expire-min", dest="db.expire.min", type=int, help="(Optional) minimum MongoDB TTL expiry in seconds")
        parser.add_argument("-X", "--db-expire-max", dest="db.expire.max", type=int, help="(Optional) maximum MongoDB TTL expiry in seconds")
        parser.add_argument("-k", "--twitter-consumer-key", dest="twitter.consumer.key", help="Twitter Consumer Key (REQUIRED)")
        parser.add_argument("-s", "--twitter-consumer-secret", dest="twitter.consumer.secret", help="Twitter Consumer Secret (REQUIRED)")
        parser.add_argument("-T", "--twitter-access-token", dest="twitter.access.token", help="Twitter Access Token (REQUIRED)")
        parser.add_argument("-S", "--twitter-access-secret", dest="twitter.access.secret", help="Twitter Access Secret (REQUIRED)")
        parser.add_argument("-F", "--twitter-filters", dest="twitter.filters", help="Comma-separated list of filters to Twitter stream (default: @realDonaldTrump)")
        parser.add_argument("-l", "--log-file", dest="log_file", help="Output to log file", type=str, default=None)
        parser.add_argument("-v", "--verbose", dest="verbose", help="Verbose/debug output (default: false)", default=False, action="store_true")
        return parser


class Smallhands():
    def __init__(self, log_level=logging.INFO):
        self.log_level = log_level
        self.config    = self.parse_config()
        self.logger    = self.setup_logger()
        self.db_conn   = None
        self.stopped   = False
        self.stream    = None

        # Parse twitter-stream filters
        self.stream_filters = ["@realDonaldTrump"]
        if 'filters' in self.config.twitter:
            self.stream_filters = self.config.twitter.filters.split(",")
        if len(self.stream_filters) < 1:
            self.logger.fatal("No Twitter stream filters!")
            sys.exit(1)

        self.logger.info("Starting Smallhands version: %s (https://github.com/timvaillancourt/smallhands)" % __VERSION__)
        self.logger.info("\t\t\"I'm going to make database testing great again. Believe me.\"")

    def setup_logger(self):
        try:
            if self.config.verbose:
                if not isinstance(self.config.verbose, str) or not self.config.verbose.startswith("false"):
                    self.log_level = logging.DEBUG 
            self.logger = logging.getLogger(__name__)
            stream_log  = logging.StreamHandler()
            formatter   = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(threadName)s] [%(module)s:%(lineno)d] %(message)s')
            stream_log.setFormatter(formatter)
            self.logger.addHandler(stream_log)
            if self.config.log_file:
                file_log = logging.FileHandler(self.config.log_file)
                file_log.setFormatter(formatter)
                self.logger.addHandler(file_log)
            self.logger.setLevel(self.log_level)
            return self.logger
        except Exception, e:
            print('Error setting up logger: %s' % e)
            sys.exit(1)

    def auth_conn(self, conn):
        try:
            if 'user' in self.config.db and 'password' in self.config.db:
                self.logger.debug("Authenticating db connection as user: %s" % self.config.db.user)
                conn[self.config.db.authdb].authenticate(
                    self.config.db.user,
                    self.config.db.password,
                    source=self.config.db.authdb
                )
        except Exception, e:
            self.logger.fatal("Error authenticating to: %s:%i - %s" % (self.config.db.host, self.config.db.port, e))

    def ensure_indices(self, conn, collection, sharded=False):
        try:
            db = conn[self.config.db.name]
            self.logger.debug("Ensuring 'id' index on '%s.%s'" % (self.config.db.name, collection))
            db[collection].create_index([("id", pymongo.ASCENDING)], unique=True)
            if sharded:
                self.logger.debug("Ensuring optional hashed 'id' index on '%s.%s' (for sharding)" % (self.config.db.name, collection))
                db[collection].create_index([('id', pymongo.HASHED)])
            if 'expire' in self.config.db and 'min_secs' in self.config.db.expire and 'max_secs' in self.config.db.expire:
                self.logger.debug("Ensuring optional TTL-expiry 'expire_at' index on '%s.%s'" % (self.config.db.name, collection))
                db[collection].create_index([("expire_at", pymongo.ASCENDING)], expireAfterSeconds=0)
        except Exception, e:
            self.logger.fatal("Error creating collections on: '%s.%s' - %s" % (self.config.db.name, collection, e))
            raise e

    def get_db(self):
        try:
            self.logger.debug("Getting mongo connection to '%s:%i'" % (self.config.db.host, self.config.db.port))
            self.db_conn = pymongo.MongoClient(
                self.config.db.host,
                self.config.db.port
            )
            self.auth_conn(self.db_conn)

            # Setup indices, setup TTL index conditionally:
            if self.db_conn.is_mongos:
                self.logger.info("Detected 'mongos' process, enabling sharding of smallhands documents")

                try:
                    self.logger.debug("Enabling sharding of db: %s" % self.config.db.name)
                    db = self.db_conn['admin']
                    db.command({ 'enableSharding': self.config.db.name })
                except Exception, e:
                    if not e.message.startswith("sharding already enabled for database"):
                        raise e

                shard_conn = None
                try:
                    db = self.db_conn['config']
                    for shard in db.shards.find():
                        shard_conn = pymongo.MongoClient(shard['host'], replicaSet=shard['_id'], readPreference='primary')
                        self.auth_conn(shard_conn)
                        self.logger.info("Checking indices on shard: %s" % shard['host'])
                        for collection in ['tweets', 'users']:
                            self.logger.debug("Ensuring shard indices for '%s.%s'" % (self.config.db.name, collection))
                            self.ensure_indices(shard_conn, collection, True)
                except Exception, e:
                    raise e
                finally:
                    if shard_conn:
                        shard_conn.close()

                for collection in ['tweets', 'users']:
                    try:
                        self.logger.debug("Sharding collection '%s.%s'" % (self.config.db.name, collection))
                        db = self.db_conn['admin']
                        db.command({ 'shardCollection': '%s.%s' % (self.config.db.name, collection), 'key': { 'id': HASHED } })
                    except Exception, e:
                        if not e.message.startswith("sharding already enabled for collection"):
                            raise e

                db = self.db_conn[self.config.db.name]
            else:
                db = self.db_conn[self.config.db.name]
                self.logger.info("Checking indices on: '%s:%i'" % (self.config.db.host, self.config.db.port))
                for collection in ['tweets', 'users']:
                    self.logger.debug("Ensuring indices for '%s.%s'" % (self.config.db.name, collection))
                    self.ensure_indices(self.db_conn, collection)
            return db
        except Exception, e:
            self.logger.fatal("Error setting up db: %s" % e)
            raise e

    def get_twitter_auth(self):
        try:
            self.logger.debug("Authenticating Twitter API session")
            auth = OAuthHandler(
                self.config.twitter.consumer.key,
                self.config.twitter.consumer.secret
            )
            auth.set_access_token(
                self.config.twitter.access.key,
                self.config.twitter.access.secret
            )
            return auth
        except Exception, e:
            self.logger.fatal("Error with Twitter auth: %s" % e)
            raise e

    def parse_config(self):
        try:
            config = SmallhandsConfig()
            config.parse(sys.argv[1:])

            # Set defaults:
            if 'db' not in config:
                config.db = {}
            if 'name' not in config.db:
                config.db.name = "smallhands"
            if 'host' not in config.db:
                config.db.host = "localhost"
            if 'port' not in config.db:
                config.db.port = 27017

            # Required fields
            if 'twitter' in config:
                required = [
                    config.twitter.access.key,
                    config.twitter.access.secret,
                    config.twitter.consumer.key,
                    config.twitter.consumer.secret
                ]
                for field in required:
                    try:
                        test = field
                    except Exception, e:
                        raise Exception, 'Required config field "%s" not specified!' % field, None
            else:
                raise Exception, 'Required "twitter" auth key/secret info not set! Please set via config file or command line!', None
            return config
        except Exception, e:
            print("Error parsing config: %s" % e)
            raise e

    def start_stream(self):
        db   = self.get_db()
        auth = self.get_twitter_auth()

        # Start the stream
        try:
            self.logger.info("Listening to Twitter Streaming API for mentions of: %s, writing data to: '%s:%i'" % (self.stream_filters, self.config.db.host, self.config.db.port))
            self.stream = Stream(auth, SmallhandsListener(db, self.config))
            self.stream.filter(track=self.stream_filters, async=True)
        except Exception, e:
            raise e

    def start(self):
        try:
            self.start_stream()
            while not self.stopped and self.stream:
                if not self.stopped and not self.stream.running:
                    self.logger.error("Restarting streaming due to error!")
                    self.stop_stream()
                    self.start_stream()
                else:
                    sleep(1)
        except Exception, e:
            self.logger.fatal("Error with Twitter Streaming: %s" % e)
            raise e

    def stop_stream(self):
        try:
            self.logger.info("Stopping tweet streaming")
            if self.stream:
                self.stream.disconnect()
        except Exception, e:
            self.logger.fatal("Error stopping streaming: %s" % e)
            raise e

    def stop(self, frame=None, code=None):
        try:
            if not self.stopped:
                self.logger.info("Smallhands stopped. Sad!")
                self.stopped = True
                self.stop_stream()
                if self.db_conn:
                    self.db_conn.close()
        except Exception, e:
            raise e


if __name__ == "__main__":
    smallhands = None
    try:
        smallhands = Smallhands()
        signal.signal(signal.SIGINT, smallhands.stop)
        signal.signal(signal.SIGTERM, smallhands.stop)
        smallhands.start()
    except Exception, e:
        sys.exit(1)
    finally:
        if smallhands:
            smallhands.stop()
