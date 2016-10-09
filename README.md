# smallhands
A simple load generator for MongoDB using mentions of @realDonaldTrump as a source w/Twitter Streaming API.

## Required
1. A Twitter account
2. A set of read-only "Twitter Apps" keys (*See: Twitter Apps Keys steps below*)
3. Python 2.7 (*Python 2.6 and 3.x not tested*)
4. Pip dependencies specified in 'requirements.txt'

### Twitter Apps Keys

Smallhands requires Twitter API auth keys. You will need to register a new read-only "app" at https://apps.twitter.com to get a set of Consumer and Access auth key pairs.

## Getting Started
```
make
cp example.yml config.yml
# (edit config.yml for your situation)
make run
```

## The Future...
1. Better, trump-themed errors/handling
2. Separate fetching and writing thread(s)
3. Optional TTL expiry on Tweets (*using MongoDB TTL Index*)

##  Code
I beat Python with a hammer until it works. Your improvements are appreciated!

## Contact
Tim Vaillancourt - [Github](https://github.com/timvaillancourt), [Email](mailto:tim@timvaillancourt.com)
