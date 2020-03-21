from tweepy.streaming import StreamListener
from tweepy import Stream
from tweepy import OAuthHandler
import json

consumer_key = "fill your consumer_key"
consumer_secret = "fill your consumer_secret"
access_token = "fill your access_token"
access_secret= "fill your access_secret"


class LiveTweetStreaming(StreamListener):
    def __init__(self,tweetsFileName):
        self.tweetsFileName = tweetsFileName

    def on_data(self,raw_data):
        try:
            tweet = json.loads(raw_data)
            text = tweet['text']
            print(text)
            with open("C://Users//karthik//Pictures//loadspark//" + tweetsFilename, 'ab') as file:
              file.write(text.encode('utf-8'))

        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_status(self,status_code):
        print(status_code)


class StreamProcessor():
    def __init__(self):
        pass

    def stream_tweets(self, tweetsFilename, hash_tag_list):
        listener = LiveTweetStreaming(tweetsFilename)
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)
        stream = Stream(auth, listener)
        stream.filter(languages=['en'],track=hash_tag_list)

if __name__== "__main__":
        hash_tag_list = ['Microsoft', 'Google', 'Apple']
        tweetsFilename = "tweets.json"
        twitter_streamer = StreamProcessor()
        twitter_streamer.stream_tweets(tweetsFilename, hash_tag_list)
