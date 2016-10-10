#!/usr/bin/env python

from datetime import timedelta
from dateutil import parser
from pymongo import MongoClient, ASCENDING
from random import randint
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
			self.count += 1
			if (self.count % 50) == 0:
				print "Wrote 50 tweets (total: %i)" % self.count
		except Exception, e:
			return SmallhandsError("Error writing tweet to db: %s" % e)

	def on_error(self, e):
		msg = e
		if e == 401:
			msg = "Unauthorized: Authentication credentials were missing or incorrect."
		return SmallhandsError("Twitter Streaming API error: '%s'" % msg, True)


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
			db['tweets'].create_index([("id", ASCENDING), ("created_at", ASCENDING)])
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
			print("Listening to Twitter Streaming API for mentions of: %s"  % self.stream_filters)
			twitterStream = Stream(auth, SmallhandsListener(db, self.config))
			twitterStream.filter(track=self.stream_filters, async=True)
		except Exception, e:
			return SmallhandsError("Error with Twitter Streaming: %s" % e, True)


if __name__ == "__main__":
	Smallhands().start()
