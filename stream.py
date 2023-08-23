import logging
import tweepy
import creds as cr
import json
from confluent_kafka import Producer
import socket
from confluent_kafka import Consumer, KafkaException

logging.basicConfig(level=logging.DEBUG)

# class MyStreamListener(tweepy.StreamListener):
#     def on_status(self, status):
#         print("working")
#         print(status.text)

auth = tweepy.OAuthHandler(cr.consumer_key, cr.consumer_secret)
auth.set_access_token(cr.access_token, cr.access_token_secret)

# Create StreamListener instance
# stream_listener = MyStreamListener()

# Create Stream object with authentication and listener
# stream = tweepy.Stream(auth=auth, listener=stream_listener)

# Start streaming with desired parameters
# try:
#     stream.filter(track=['technology'], is_async=True)
# except Exception as e:
#     print(f"Streaming error: {str(e)}")

api = tweepy.API(auth)

# public_tweets = api.home_timeline(count=5)  # Adjust the count as per your requirement

# for tweet in public_tweets:
#     print(tweet.text)

# Batch polling parameters
batch_size = 1 # Number of tweets to retrieve per batch
total_tweets = 5  # Total number of tweets to retrieve

tweets = []
max_id = None

while len(tweets) < total_tweets:
    batch_count = min(batch_size, total_tweets - len(tweets))
    results = api.home_timeline(count=batch_count, max_id=max_id)

    if not results:
        break

    tweets.extend(results)
    max_id = results[-1].id - 1

# Create a dictionary with tweet ID as the key and tweet text as the value
tweet_dict = {tweet.id_str: tweet.text for tweet in tweets}

conf = {
    'bootstrap.servers': "localhost:9092",
    'client.id': socket.gethostname()
}

producer = Producer(conf)

for tweet in tweets:
    tweet_json = json.dumps(tweet._json)  # Convert the tweet object to JSON string
    producer.produce('twitter_topic', value=tweet_json.encode('utf-8'))  # Send the tweet to Kafka
    producer.flush()  # Flush the messages to ensure they are sent immediately

# Save the tweet dictionary to a JSON file
with open('tweets.json', 'w') as f:
    json.dump(tweet_dict, f, indent=4)

# Print the tweets
for tweet in tweets:
    print(tweet.text)

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

# Subscribe to the 'twitter_topic' Kafka topic
consumer.subscribe(['twitter_topic'])

try:
    while True:
        msg = consumer.poll(1.0)  # Poll for new messages from Kafka
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                # End of partition event, ignore
                continue
            else:
                # Error occurred
                print('Consumer error: {}'.format(msg.error()))
                break
        else:
            # Message received successfully
            tweet_json = msg.value().decode('utf-8')
            tweet = json.loads(tweet_json)
            print(tweet['text'])  # Print the tweet text

except KeyboardInterrupt:
    pass

finally:
    # Close the Kafka consumer
    consumer.close()

