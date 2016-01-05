from __future__ import print_function
from urllib.request import urlopen
from bs4 import BeautifulSoup
from sklearn.datasets import fetch_20newsgroups
from bson import json_util
import json
import datetime
import pymongo
import sys
import os
import nltk
import pickle
from senti_classifier import senti_classifier
from nltk.corpus import sentiwordnet as swn
from prettytable import PrettyTable
from prettytable import MSWORD_FRIENDLY
from collections import Counter
from string import punctuation
from time import time

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF


def get_coll(mongo_db, mongo_db_coll, **mongo_conn_kw):
    client = pymongo.MongoClient(**mongo_conn_kw)
    db = client[mongo_db]
    coll = db[mongo_db_coll]
    return coll

def get_extract():
    coll = get_coll('twitter','stream')
    pipeline = [
         { "$match": { "entities": {'$exists': True }} },
         { "$match": { "where": "this.entities.urls.length > 1" } },
         { "$sort": { "natural": -1 } },
	 { "$limit": 2000 }
    ]
    tweets = coll.aggregate( pipeline )
    return tweets

def do_classify():
  n_samples = 2000
  n_features = 1000
  n_topics = 10
  n_top_words = 3
  tweets = get_extract()
  tweet_texts = fetch_tweet_texts(tweets)
  vectorizer = TfidfVectorizer(max_df=0.95,min_df=2, max_features=n_features,
                             stop_words='english')
  tfidf = vectorizer.fit_transform(tweet_texts[:n_samples])
  nmf = NMF(n_components=n_topics, random_state=1).fit(tfidf)
  feature_names = vectorizer.get_feature_names()
  for topic_idx, topic in enumerate(nmf.components_):
    print("Topic #%d:" % topic_idx)
    print(" ".join([feature_names[i]
                    for i in topic.argsort()[:-n_top_words - 1:-1]]))
    print()


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

def fetch_tweet_texts(tweets):
  tweets_list = []
  for tweet in tweets:
    if 'text' in tweet:
      text = tweet['text']
    else:
      text = ''
    web_text = ''
    if 'entities' in tweet:
      if 'urls' in tweet['entities']:
        for url in tweet['entities']['urls']:
          print('Fetching ' + url['url'])
          try:
      	    html_doc = urlopen(url['url']).read()   
          except:
            web_text = ''
          else:
            soup = BeautifulSoup(html_doc, 'html.parser')
            web_text = soup.get_text()
    full_text = text + ' ' + web_text
    tweets_list.append(full_text)
  return tweets_list

def dump_tweets():
    tweets = get_extract()
    tweets_list = []
    for tweet in tweets:
      for url in tweets.entities.urls:
        html_doc = urlopen(url['url']).read()   
        soup = BeautifulSoup(html_doc, 'html.parser')
        web_text = soup.get_text()
        full_text = tweet + web_text
      tweets_list.append(full_text) 
    print(len(tweets_list))
    print_extract(tweets_list)
    

def main():
  do_classify()
  #dump_tweets()

if __name__ == "__main__": main()
