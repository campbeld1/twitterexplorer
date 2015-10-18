from twitter import *
import json
import pymongo
import sys
import os
from prettytable import PrettyTable
from collections import Counter
import local_settings as settings

def save_to_mongo(data, mongo_db, mongo_db_coll, **mongo_conn_kw):
    client = pymongo.MongoClient(**mongo_conn_kw)
    db = client[mongo_db]
    coll = db[mongo_db_coll]
    return coll.insert(data)

def get_tweets():
  CONSUMER_KEY=settings.CONSUMER_KEY
  CONSUMER_SECRET=settings.CONSUMER_SECRET


  lang='en'
  locations='112.2,-44.5,154.9,-8.0'

  if not os.path.exists("/root/tokens"):
    oauth_dance("DCWDMyTestApp", CONSUMER_KEY, CONSUMER_SECRET,"/root/tokens")

  oauth_token, oauth_secret = read_token_file("/root/tokens")

  auth=OAuth(oauth_token, oauth_secret, CONSUMER_KEY, CONSUMER_SECRET)

  twitter_stream = TwitterStream(auth=auth)
  stream = twitter_stream.statuses.filter(locations=locations,language=lang)
  for tweet in stream:
    save_to_mongo(tweet, 'twitter', 'stream')

def main():
  get_tweets()

if __name__ == "__main__": 
  main()
