import tweepy
import creds as cr
from tweepy import OAuthHandler

class MyStreamListener(tweepy.streaming.StreamListener):
    def on_status(self, status):
        print(status.text)

# auth = tweepy.OAuth1UserHandler(cr.consumer_key, cr.consumer_secret, cr.access_token, cr.access_token_secret)

# # Create StreamListener instance
# stream_listener = MyStreamListener(cr.bearer_token)

# # Create Stream object with authentication and listener
# stream = Stream(auth=auth, listener=stream_listener)

# # Start streaming with desired parameters
# stream.filter(track=['#GTvsCSK'], is_async=True)

auth = OAuthHandler(cr.consumer_key, cr.consumer_secret)
auth.set_access_token(cr.access_token, cr.access_token_secret)

twitterStream = tweepy.Stream(auth, MyStreamListener())
twitterStream.filter(track=['#GTvsCSK'], is_async=True)

import tweepy

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print(status.text)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

myStream.filter(track=['python'])
