from bson import json_util
import json
import datetime
import pymongo
import sys
import os
import daemon
import nltk
import pickle
from senti_classifier import senti_classifier
from nltk.corpus import sentiwordnet as swn
from prettytable import PrettyTable
from prettytable import MSWORD_FRIENDLY
from collections import Counter
from string import punctuation

def get_positive_words():
   return ['good', 'nice', 'super', 'fun', 'delightful', 'like']

def get_negative_words():
   return ['awful','lame','horrible','bad']

def get_emotional_words():
   return get_positive_words()+get_negative_words()

def remove_punctuation(text):
   text_processed=text
   for p in list(punctuation):
     text_processed=text_processed.replace(p,'')
   return text_processed

def classify(text):
    positive=0
    negative=0
    words = text.split(' ')
    for word in words:
      if word in get_positive_words():
        positive+=1 
      if word in get_negative_words():
        negative+=1
    if positive==0 and negative==0:
      return ''
    if positive>=negative:
      return 'positive'
    else:
      return negative
        
 

def get_coll(mongo_db, mongo_db_coll, **mongo_conn_kw):
    client = pymongo.MongoClient(**mongo_conn_kw)
    db = client[mongo_db]
    coll = db[mongo_db_coll]
    return coll

def prepare_dates():
    coll = get_coll('twitter','stream')
    tweets = coll.find({ 'created_at_date': { '$exists': False }})
    print(tweets.count())
    for tweet in tweets:
      if 'created_at' in tweet:
        thedate = tweet['created_at']
        proper_date = datetime.datetime.strptime(thedate,'%a %b %d %H:%M:%S +0000 %Y')
        pointer = tweet['_id']
        coll.update({'_id': pointer}, {'$set': {'created_at_date': proper_date}})
   

def get_extract():
    coll = get_coll('twitter','stream')
    d = datetime.datetime(2015, 10, 8, 8, 40)
    pipeline = [
         { "$sort": { "created_at_date": -1 } },
	 { "$match": { "$and": [ { "created_at_date": { "$exists": "true" } }, { "created_at_date": {"$gt":d} }]}},
	 { "$limit": 10 }
    ]
    tweets = coll.aggregate( pipeline )
    return tweets

def print_extract(tweets):
    x = PrettyTable(["Date", "Pos", "Neg", "Sentiment", "Text"]) 
    x.align["Text"] = "l"
    x.set_style(MSWORD_FRIENDLY)

    counter = 0

    for tweet in tweets:
      if 'text' in tweet:
        text = tweet['text']
      else:
        text = ''

      sentiment=classify(text)
      pos_score, neg_score = senti_classifier.polarity_scores([text])
      x.add_row([ tweet['created_at_date'], pos_score, neg_score, sentiment, text ])
      counter+=1
      print(str(counter)+'/'+str(len(tweets)))

    print(x)

def dump_tweets():
    tweets = get_extract()
    tweets_list = []
    for tweet in tweets:
      tweets_list.append(tweet) 
    print(len(tweets_list))
    print_extract(tweets_list)
    

def daemonize():
  context = daemon.DaemonContext()
  context.open()
  with context:
    dump_tweets()

def main():
  prepare_dates()
  dump_tweets()

if __name__ == "__main__": main()
