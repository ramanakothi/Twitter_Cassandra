#!/usr/bin/env python

import psycopg2
import tweepy 
import json
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider
from cassandra import InvalidRequest as CE
from keys import *



def autorize_twitter_api():
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth

def connect_cassandra():
    auth_provider = PlainTextAuthProvider(username=cassandrauser,password=casspassword)
    cluster = Cluster([host_list],protocol_version=4,
                      auth_provider = auth_provider,port=cassandra_port)
    session = cluster.connect()
    session.set_keyspace('twitter')
    return  session

cursor_twitter = connect_cassandra()

def create_tweets_table_cassandra(term_to_search):
    """
    This function open a connection with an already created database and creates a new table to
    store tweets related to a subject specified by the user
    """
    query_create = "CREATE TABLE IF NOT EXISTS %s (id UUID PRIMARY KEY, created_at text, tweet text, user_id text,\
                    retweetcount int, location text, place text, fullmessage text);" %("tweets_"+term_to_search)
    cursor_twitter.execute(query_create)

    return

def store_tweets_in_table_cassandra(term_to_search, user_id, created_at, tweet, user_name, retweetcount,location, place, data):
    """
    This function open a connection with an already created database and inserts into corresponding table 
    tweets related to the selected topic
    """
    cursor_twitter.execute("INSERT INTO %s (id,created_at, tweet, user_id, retweetcount, location, place,fullmessage) VALUES (now(),%%s, %%s, %%s, %%s,%%s,%%s, %%s);" %('tweets_'+term_to_search), (created_at, tweet, user_id, retweetcount, location, place, data))

    return

class MyStreamListener(tweepy.StreamListener):
    def on_data(self, raw_data):
        try:
            global term_to_search
            data = json.loads(raw_data)            
            user_id = data['user']['id_str']
            created_at = data['created_at']
            tweet = data['text']
            user_name = data['user']['screen_name']
            retweetcount = data['retweet_count']
            if data['place'] is not None:
                place = data['place']['country']
            else:
                place = None
            location = data['user']['location']
            
            #Store them in the Cassandra table
            store_tweets_in_table_cassandra(term_to_search, user_id, created_at, tweet, user_name, 
                                            retweetcount, location, place, raw_data)            
        except Exception as e:
            pass
    
    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False


if __name__ == "__main__": 
    #Creates the table for storing the tweets
    term_to_search = "coronavirus"
    create_tweets_table_cassandra(term_to_search)
    
    #Connect to the streaming twitter API
    api = tweepy.API(wait_on_rate_limit_notify=True)
    
    #Stream the tweets
    streamer = tweepy.Stream(auth=autorize_twitter_api(), listener=MyStreamListener(api=api))
    streamer.filter(languages=["en"], track=[term_to_search])
    #streamer.filter(languages=["en"],track=['$BABA', '$TCEHY', '$BIDU', '$AAPL', '$TSLA','Trump','Barnie','Warren'])
