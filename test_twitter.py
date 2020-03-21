from tweepy.streaming import StreamListener 
from tweepy import Stream
from tweepy import OAuthHandler 
import json

# Set up your credentials

consumer_key = "Please add Your Your consumer_key in Quotes"
consumer_secret = "Please add Your consumer_secret in Quotes"
access_token = "Please add Your access_token here in Quotes"
access_secret= "Please add Your access_secret in Quotes"

# Class for streaming and processing live tweets.

class LiveTweetStreaming(StreamListener):
    def __init__(self,tweetsFileName):
        self.tweetsFileName = tweetsFileName

# Below function is a basic listener that just prints received tweets to stdout

    def on_data(self,raw_data):
        try:
            tweet = json.loads(raw_data)
            text = tweet['text']
            print(text)

        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

# Below function prints error status_code if anything goes wrong with streaming

    def on_status(self,status_code):
        print(status_code)


class StreamProcessor():
    def __init__(self):
        pass

# This handles Twitter authetification and the connection to Twitter Streaming API

    def stream_tweets(self, tweetsFilename, hash_tag_list):
        listener = LiveTweetStreaming(tweetsFilename)
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)
        stream = Stream(auth, listener)
        # Below line filter Twitter Streams to capture data by the keywords also you can languages: 
        stream.filter(languages=['en'],track=hash_tag_list) 

if __name__== "__main__":
        hash_tag_list = ['Microsoft', 'Google', 'Apple']
        tweetsFilename = "tweets.json"
        twitter_streamer = StreamProcessor()
        twitter_streamer.stream_tweets(tweetsFilename, hash_tag_list)