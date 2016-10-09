# smallhands
A simple load generator for MongoDB using mentions of @realDonaldTrump as a source w/Twitter Streaming API.

## Required
1. A Twitter account
2. A set of "Twitter Apps" keys (*Consumer Key+Secret and Access Key+Secret*). Receive keys via registering an "app" at https://apps.twitter.com
3. Python 2.7 (*Python 2.6 and 3.x not tested*)
4. Pip dependencies specified in 'requirements.txt'

## Getting Started
```
make
cp example.yml config.yml
# (edit config.yml for your situation)
make run
```

## The Future...
1. Better errors/handling
2. Separate fetching and writing threads

##  Code
I beat Python with a hammer until it works. Your improvements are appreciated!

## Contact
Tim Vaillancourt - [Github](https://github.com/timvaillancourt), [Email](mailto:tim@timvaillancourt.com)
