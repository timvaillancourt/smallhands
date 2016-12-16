from yconf import BaseConfiguration


class SmallhandsConfig(BaseConfiguration):
    def makeParser(self):
        parser = super(SmallhandsConfig, self).makeParser()
        parser.add_argument("-v", "--verbose", dest="verbose", help="Verbose/debug output (default: false)", default=False, action="store_true")
        parser.add_argument("-l", "--log-file", dest="log_file", help="Output to log file", type=str, default=None)
        parser.add_argument("-R", "--report-interval", dest="report_interval", help="Interval in seconds for report updates", type=int, default=30)
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
        parser.add_argument("--twitter-stream-timeout", dest="twitter.stream.timeout", help="Timeout for Twitter Streaming API, in seconds (default: 30)", default=30, type=int)
        return parser
