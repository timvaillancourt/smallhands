#!/usr/bin/env python

from datetime import timedelta
from dateutil import parser
from pymongo import MongoClient, ASCENDING, HASHED
from random import randint
from time import sleep
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
from yconf import BaseConfiguration

import json
import sys

__VERSION__ = "0.2"
	

class SmallhandsError():
	def __init__(self, error, do_exit=False):
		print("I know errors. I've got the best errors:\t\"%s\"" % error)
		if do_exit:
			sys.exit(1)


class SmallhandsListener(StreamListener):
	def __init__(self, db, config):
		self.db     = db
		self.config = config
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
                        return SmallhandsError("Error parsing tweet: %s" % e)

	def process_tweet(self, data):
		try:
			tweet = self.parse_tweet(json.loads(data))
			if 'created_at' in tweet and 'expire' in self.config.db:
				if 'min_secs' in self.config.db.expire and 'max_secs' in self.config.db.expire:
					ttl_secs = randint(self.config.db.expire.min_secs, self.config.db.expire.max_secs)
					tweet['expire_at'] = tweet['created_at'] + timedelta(seconds=ttl_secs)
			return tweet
                except Exception, e:
                        return SmallhandsError("Error processing tweet: %s" % e)

	def on_data(self, data):
		tweet = self.process_tweet(data)
		try:
			self.db['tweets'].insert(tweet)
			if 'user' in tweet:
				self.db['users'].update_one({ 'id': tweet['user']['id'] }, { '$set' : tweet['user'] }, upsert=True)
			self.count += 1
			if (self.count % 50) == 0:
				print "Wrote 50 tweets (total: %i)" % self.count
		except Exception, e:
			if not e.message.startswith("E11000 duplicate key"):
				return SmallhandsError("Error writing tweet to db: %s" % e)

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
		return SmallhandsError("Twitter Streaming API error: '%s'" % msg, True)

	def on_exception(self, exception):
		print 'got exception: %s' % exception
		sleep(1)


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
		return parser


class Smallhands():
	def __init__(self):
		self.config  = self.parse_config()
		self.db_conn = None
		self.active_streams = []

		# Parse twitter-stream filters
		self.stream_filters = ["@realDonaldTrump"]
		if 'filters' in self.config.twitter:
			self.stream_filters = self.config.twitter.filters.split(",")
		if len(self.stream_filters) < 1:
			SmallhandsError("No Twitter stream filters!", True)

		print("# Starting Smallhands version: %s (https://github.com/timvaillancourt/smallhands)" % __VERSION__)
		print("#   \"I'm going to make database testing great again. Believe me.\"")
		print("# Vote (if you can): www.rockthevote.com!!!\n")

	def get_db(self):
		try:
			self.db_conn = MongoClient(
				self.config.db.host,
				self.config.db.port
			)
			db = self.db_conn[self.config.db.name]

			# Authentication:
			if 'user' in self.config.db and 'password' in self.config.db:
				db.authenticate(
					self.config.db.user,
					self.config.db.password,
					source=self.config.db.authdb
				)

			# Setup indices, setup TTL index conditionally:
			if self.db_conn.is_mongos:
				print "Detected 'mongos' process, enabling sharding of smallhands documents"

				try:
					db = self.db_conn['admin']
					db.command({ 'enableSharding': self.config.db.name })
				except Exception, e:
					if not e.message.startswith("sharding already enabled for database"):
						raise e

				shard_conn = None
				try:
					db = self.db_conn['config']
					for shard in db.shards.find():
						shard_conn = MongoClient(shard['host'])
					        if 'user' in self.config.db and 'password' in self.config.db:
							shard_db = shard_conn[self.config.db.authdb]
                                			shard_db.authenticate(
		                                	        self.config.db.user,
                		                        	self.config.db.password,
	                                		        source=self.config.db.authdb
			                                )
						shard_db = shard_conn[self.config.db.name]
						shard_db['tweets'].create_index([("id", ASCENDING)], unique=True)
						shard_db['users'].create_index([("id", ASCENDING)], unique=True)
						shard_db['tweets'].create_index([('id', HASHED)])
						shard_db['users'].create_index([('id', HASHED)])
						if 'expire' in self.config.db and 'min_secs' in self.config.db.expire and 'max_secs' in self.config.db.expire:
							shard_db['tweets'].create_index([("expire_at", ASCENDING)], expireAfterSeconds=0)
				except Exception, e:
					raise e
				finally:
					if shard_conn:
						shard_conn.close()

				try:
					db = self.db_conn['admin']
					db.command({ 'shardCollection': '%s.tweets' % self.config.db.name, 'key': { 'id': HASHED } })
					db.command({ 'shardCollection': '%s.users' % self.config.db.name, 'key': { 'id': HASHED } })
				except Exception, e:
					if not e.message.startswith("sharding already enabled for collection"):
						raise e

				db = self.db_conn[self.config.db.name]
			else:
				db['tweets'].create_index([("id", ASCENDING)], unique=True)
				db['users'].create_index([("id", ASCENDING)], unique=True)
				if 'expire' in self.config.db and 'min_secs' in self.config.db.expire and 'max_secs' in self.config.db.expire:
					db['tweets'].create_index([("expire_at", ASCENDING)], expireAfterSeconds=0)
			return db
		except Exception, e:
			return SmallhandsError("Error setting up db: %s" % e, True)

	def get_twitter_auth(self):
		try:
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
			return SmallhandsError("Error with Twitter auth: %s" % e, True)

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
			return SmallhandsError("Error parsing config: %s" % e, True)

	def start(self):
		db   = self.get_db()
		auth = self.get_twitter_auth()

		# Start the stream
		try:
			print("Listening to Twitter Streaming API for mentions of: %s, writing data to: '%s:%i'" % (self.stream_filters, self.config.db.host, self.config.db.port))
			stream = Stream(auth, SmallhandsListener(db, self.config))
			stream.filter(track=self.stream_filters, async=True)
			self.active_streams.append(stream)
			return self.wait_streams()
		except Exception, e:
			return SmallhandsError("Error with Twitter Streaming: %s" % e, True)

	def wait_streams(self):
		while len(self.active_streams) > 0:
			for stream in self.active_streams:
				if not stream.running:
					self.active_streams.remove(stream)
				else:
					sleep(1)

	def stop(self):
		print("\nStopping stream and exiting Smallhands. Sad!")
		if len(self.active_streams) > 0:
			for stream in self.active_streams:
				stream.disconnect()
			self.wait_streams()
		if self.db_conn:
			self.db_conn.close()


if __name__ == "__main__":
        smallhands = None
	try:
		smallhands = Smallhands()
		smallhands.start()
	except:
		if smallhands:
			smallhands.stop()
