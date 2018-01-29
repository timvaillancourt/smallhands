import logging
import pymongo
import signal
import sys

import smallhands.config as cnf
import smallhands.listener as listener

from time import sleep
from tweepy import Stream, OAuthHandler


__VERSION__ = "0.6"


class Smallhands():
    def __init__(self, log_level=logging.INFO):
        self.log_level = log_level
        self.config    = self.parse_config()
        self.logger    = self.setup_logger()
        self.db_conn   = None
        self.stopped   = False
        self.stream    = None

        # sig handlers
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        # Parse twitter-stream filters
        self.stream_filters = ["@realDonaldTrump", "@POTUS"]
        if 'filters' in self.config.twitter:
            self.stream_filters = self.config.twitter.filters.split(",")
        if len(self.stream_filters) < 1:
            self.logger.fatal("No Twitter stream filters!")
            raise (Exception, "No Twitter stream filters!", None)

    def setup_logger(self):
        try:
            if self.config.verbose:
                if not isinstance(self.config.verbose, str) or self.config.verbose.startswith("true"):
                    self.log_level = logging.DEBUG
            self.logger = logging.getLogger()
            stream_log  = logging.StreamHandler()
            formatter   = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(threadName)s] [%(module)s.%(funcName)s:%(lineno)d] %(message)s')
            stream_log.setFormatter(formatter)
            self.logger.addHandler(stream_log)
            if self.config.log_file:
                file_log = logging.FileHandler(self.config.log_file)
                file_log.setFormatter(formatter)
                self.logger.addHandler(file_log)
            self.logger.setLevel(self.log_level)
            return self.logger
        except Exception as e:
            print('Error setting up logger: %s' % e)
            raise e

    def auth_conn(self, conn):
        try:
            if 'user' in self.config.db and 'password' in self.config.db:
                self.logger.debug("Authenticating db connection as user: %s" % self.config.db.user)
                conn[self.config.db.authdb].authenticate(
                    self.config.db.user,
                    self.config.db.password,
                    source=self.config.db.authdb
                )
        except Exception as e:
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
        except Exception as e:
            self.logger.fatal("Error creating collections on: '%s.%s' - %s" % (self.config.db.name, collection, e))
            raise e

    def get_db(self, force=False):
        try:
            if force or not self.db_conn:
                self.logger.debug("Getting mongo connection to '%s:%i'" % (self.config.db.host, self.config.db.port))
                self.db_conn = pymongo.MongoClient(
                    self.config.db.host,
                    self.config.db.port
                )
                self.auth_conn(self.db_conn)

                # Setup indices, setup TTL index conditionally:
                if self.db_conn.is_mongos:
                    self.logger.info("Detected 'mongos' process. Using sharded mode")

                    try:
                        self.logger.info("Ensuring sharding is enabled for db: %s" % self.config.db.name)
                        db = self.db_conn['admin']
                        db.command({'enableSharding': self.config.db.name})
                    except pymongo.errors.OperationFailure as e:
                        if e.code != 23:
                            raise e
                    except Exception as e:
                        raise e

                    shard_conn = None
                    try:
                        db = self.db_conn['config']
                        for shard in db.shards.find():
                            shard_conn = pymongo.MongoClient(shard['host'], replicaSet=shard['_id'], readPreference='primary')
                            self.auth_conn(shard_conn)
                            self.logger.info("Ensuring indices on shard: %s" % shard['host'])
                            for collection in ['tweets', 'users']:
                                self.logger.debug("Ensuring shard indices for '%s.%s'" % (self.config.db.name, collection))
                                self.ensure_indices(shard_conn, collection, True)
                    except Exception as e:
                        raise e
                    finally:
                        if shard_conn:
                            shard_conn.close()

                    for collection in ['tweets', 'users']:
                        try:
                            self.logger.info("Ensuring collection is sharded: '%s.%s'" % (self.config.db.name, collection))
                            db = self.db_conn['admin']
                            db.command({'shardCollection': '%s.%s' % (self.config.db.name, collection), 'key': {'id': pymongo.HASHED}})
                        except pymongo.errors.OperationFailure as e:
                            if e.code != 20:
                                raise e
                        except Exception as e:
                            raise e
                else:
                    self.logger.info("Ensuring indices on: '%s:%i'" % (self.config.db.host, self.config.db.port))
                    for collection in ['tweets', 'users']:
                        self.logger.debug("Ensuring indices for '%s.%s'" % (self.config.db.name, collection))
                        self.ensure_indices(self.db_conn, collection)
            return self.db_conn[self.config.db.name]
        except Exception as e:
            self.logger.fatal("Error setting up db: %s" % e)
            raise (Exception, 'MongoDB connection error - %s' % e, None)

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
        except Exception as e:
            self.logger.fatal("Error with Twitter auth: %s" % e)
            raise e

    def parse_config(self):
        try:
            config = cnf.Config()
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
                        test = field  # NOQA
                    except Exception as e:
                        raise (Exception, 'Required config field "%s" not specified!' % field, None)
            else:
                raise (Exception, 'Required "twitter" auth key/secret info not set! Please set via config file or command line!', None)
            return config
        except Exception as e:
            print("Error parsing config: %s" % e)
            raise e

    def start_stream(self):
        db   = self.get_db()
        auth = self.get_twitter_auth()

        # Start the stream
        try:
            self.logger.info("Listening to Twitter Streaming API for mentions of: %s, writing data to: '%s:%i'" % (self.stream_filters, self.config.db.host, self.config.db.port))
            self.stream = Stream(auth, listener.Listener(db, self.config), timeout=self.config.twitter.stream.timeout)
            self.stream.filter(track=self.stream_filters, async=True)
        except Exception as e:
            raise e

    def start(self):
        self.logger.info("Starting Smallhands version: %s (https://github.com/timvaillancourt/smallhands)" % __VERSION__)
        self.logger.info("\t\t\"I'm going to make database testing great again. Believe me.\"")
        try:
            self.start_stream()
            while not self.stopped and self.stream:
                if not self.stopped and not self.stream.running:
                    self.logger.error("Restarting streaming due to error!")
                    self.stop_stream()
                    self.start_stream()
                else:
                    sleep(1)
        except Exception as e:
            self.logger.fatal("Error with Twitter Streaming: %s" % e)
            raise e

    def stop_stream(self):
        try:
            self.logger.info("Stopping tweet streaming")
            if self.stream:
                self.stream.disconnect()
        except Exception as e:
            self.logger.fatal("Error stopping streaming: %s" % e)
            raise e

    def stop(self, frame=None, code=None):
        if not self.stopped:
            try:
                self.stopped = True
                self.stop_stream()
                if self.db_conn:
                    self.db_conn.close()
                self.logger.info("Smallhands stopped. Sad!")
                if frame and code:
                    sys.exit(1)
            except Exception as e:
                raise e
