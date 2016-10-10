#!/usr/bin/env python

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from pymongo import MongoClient
from yconf import BaseConfiguration

import json
import sys

__VERSION__ = "0.0.1"
	

class SmallhandsError():
	def __init__(self, error, do_exit=False):
		print("I know errors. I've got the best errors:\t\"%s\"" % error)
		if do_exit:
			exit(1)
		return False


class SmallhandsListener(StreamListener):
	def __init__(self, db):
		self.db     = db
		self.count  = 0

	def on_data(self, data):
		try:
			tweet = json.loads(data)
			self.db['tweets'].insert(tweet)
			self.count += 1
			if (self.count % 50) == 0:
				print "Wrote 50 tweets (total: %i)" % self.count
		except Exception, e:
			return SmallhandsError(e)

	def on_error(self, e):
		return SmallhandsError("Twitter error fetching tweets: '%s'" % e)


class SmallhandsConfig(BaseConfiguration):
	def makeParser(self):
		parser = super(SmallhandsConfig, self).makeParser()
		parser.add_argument("-H", "--db-host", dest="db.host", help="MongoDB hostname/ip")
		parser.add_argument("-P", "--db-port", dest="db.port", type=int, help="MongoDB TCP port")
		parser.add_argument("-D", "--db-name", dest="db.name", help="MongoDB Database name (default: smallhands)")
		parser.add_argument("-u", "--db-user", dest="db.user", help="(Optional) MongoDB Username")
		parser.add_argument("-p", "--db-password", dest="db.password", help="(Optional) MongoDB Password")
		parser.add_argument("-d", "--db-authdb", dest="db.authdb", default="admin", help="MongoDB Authentication Database (default: admin)")
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
		print("# Vote (if you can): www.rockthevote.com!!!\n")

	def get_db(self):
		try:
			self.db_conn = MongoClient(
				self.config.db.host,
				self.config.db.port
			)
			db = self.db_conn[self.config.db.name]
			if 'user' in self.config.db and 'password' in self.config.db:
				db.authenticate(
					self.config.db.user,
					self.config.db.password,
					source=self.config.db.authdb
				)
			return db
		except Exception, e:
			return SmallhandsError(e, True)

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
			return SmallhandsError(e, True)

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
			return SmallhandsError(e, True)

	def start(self):
		# Start the stream
		try:
			db   = self.get_db()
			auth = self.get_twitter_auth()
			if auth:
				twitterStream = Stream(auth, SmallhandsListener(db))
				twitterStream.filter(track=self.stream_filters)
		except Exception, e:
			return SmallhandsError(e, True)


if __name__ == "__main__":
	Smallhands().start()
