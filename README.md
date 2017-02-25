# Stream Mining
This example works on twitter data, as huge volumes of new tweets are generated every day. The repository contains several files that illustrate the different components needed. 
## Examples in repository

### "get_tweets_with_listener.py"
Implements a stream listener based on the [tweepy](http://www.tweepy.org/) library. A configuration file is read to determine: 
+ Keywords that should be watched.
+ Number of tweets to extract. If no amount is specified the listener will print all incoming tweets until the user stops the process. 
+ Format to persist the data. It is possible to store as CSV or [AVRO](https://avro.apache.org/) file. 
+ The AVRO Schema to encode the tweets. Right now a basic Schema called "tweet.avsc" is used to store the Username and the cleaned tweet text. 

### "simple_kafka_producer.py/simple_kafka_consumer.py"
Tests the possibilities of Kafka to decouple producing and consuming of messages. To do this Kafka differentiates producers, brokers and consumers:
+ Producers generate an output and send messages to a "topic".
+ Broker store those messages into a specified amount of partitions. Brokers are stateless and need Zookeeper to manage state and coordinate between different brokers.
+ Consumers access and process the messages and acknowledge a new offset afterwards. By providing a different offset older messages can be read again in case of failure. Several consumers can work on the same topic by sharing a "group_id". Different consumers can process the same topics as the brokers will keep the messages until the specified duration of retention is reached. 

To execute this example it is required to start instances of  [Zookeeper](https://zookeeper.apache.org/) and [Kafka](https://kafka.apache.org/). Zookeeper is used by Kafka to orchestrate and synchronize the work between different nodes of producers or consumers. This example uses just one producer and one consumer. Whenever the simple producer is started it connects to Kafka on port 9092, encodes the message with the Avro Schema "user.avsc" and publishes it to the "user" topic. If the consumer is already running it will display the new message as soon as it arrives. Consumers can be configured to start at the current offset or process all retained messages again. 

## Installed libraries
+ kafka 2.11-0.10.1.1
+ zookeeper 3.4.9
+ avro-python3 1.8.1
+ kafka-python 1.3.2
+ oauthlib 2.0.1
+ requests 2.13.0
+ requests-oauthlib 0.8.0
+ tweepy 3.5.0

## Zookeeper configuration / startup

## Kafka configuration / startup
