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
		print("I know errors, I've got the best errors:\t\"%s\"" % error)
		if do_exit:
			exit(1)
		return False


class SmallhandsListener(StreamListener):
	def __init__(self, config):
		self.config = config
		self.count  = 0
		try:
			self.db_conn = MongoClient(
				self.config.db.host,
				self.config.db.port
			)
			if 'user' in self.config.db and 'password' in self.config.db:
				self.db_conn[self.config.db.authdb].authenticate(
					self.config.db.user,
					self.config.db.password
				)
			self.db = self.db_conn[self.config.db.name]
		except Exception, e:
			return SmallhandsError(e, True)

	def write_nonsense(self, data):
		try:
			self.db['tweets'].insert(data)
			self.count += 1
			if (self.count % 50) == 0:
				print "Wrote %i tweets" % self.count
		except Exception, e:
			return SmallhandsError(e)

	def on_data(self, data):
		try:
			decoded = json.loads(data)
			return self.write_nonsense(decoded)
		except Exception, e:
			return SmallhandsError(e)
		
	def on_error(self, e):
		if self.db_conn:
			self.db_conn.close()
		return SmallhandsError("Error fetching tweets: '%s'" % e)


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
		self._parser = parser
		return self._parser

class Smallhands():
	def __init__(self, cmdline_args):
		self.config = self.parse_config(cmdline_args)

		# Parse twitter-stream filters
		self.stream_filters = ["@realDonaldTrump"]
		if 'twitter' in self.config and 'filters' in self.config.twitter:
			self.stream_filters = self.config.twitter.filters.split(",")
		if len(self.stream_filters) < 1:
			SmallhandsError("No Twitter stream filters!", True)

		print("# Starting Smallhands version: %s (https://github.com/timvaillancourt/smallhands)" % __VERSION__)
		print("# Enjoying the tool? No thanks necessary, just vote: www.rockthevote.com!\n")

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
			SmallhandsError(e, True)

	def parse_config(self, cmdline_args):
		try:
			config = SmallhandsConfig()
			config.parse(cmdline_args)

			# Set defaults:
			if 'name' not in config.db:
				config.db.name = "smallhands"
			if 'host' not in config.db:
				config.db.host = "localhost"
			if 'port' not in config.db:
				config.db.port = 27017

			# Required fields
			required = [
				config.twitter.access.key,
				config.twitter.access.secret,
				config.twitter.consumer.key,
				config.twitter.consumer.secret
			]
			for field in required:
				if not field:
					raise Exception, 'Required config field "%s" not specified!' % field, None
			return config
		except Exception, e:
			SmallhandsError(e, True)

	def start(self):
		# Start the stream
		try:
			auth = self.get_twitter_auth()
			if auth:
				twitterStream = Stream(auth, SmallhandsListener(self.config))
		except Exception, e:
			SmallhandsError(e, True)
		twitterStream.filter(track=self.stream_filters)

if __name__ == "__main__":
	Smallhands(sys.argv[1:]).start()
