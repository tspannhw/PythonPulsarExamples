### No Async Subscribe available yet

import time
import pulsar
import sys
import datetime
import re
from time import gmtime, strftime
import random, string
from pulsar.schema import *
from pulsar import ConsumerType, InitialPosition

# Reference https://pulsar.apache.org/api/python/2.9.0-SNAPSHOT/#pulsar.Client.subscribe
# https://github.com/apache/pulsar/blob/master/pulsar-client-cpp/python/pulsar_test.py
# Apache Pulsar Python does not have async subscription
# https://github.com/streamnative/developing_pulsar_exercise/blob/master/2_Messaging/solutions/java/src/main/java/sn_training/ex1_basics/WorkQueueConsumer.java

def my_listener(consumer, message):
    # process message
    print(message)
    consumer.acknowledge(message)

subscriptionName = "worker_queue"

topicName = "dynamic-topic-1"

client = pulsar.Client('pulsar://localhost:6650')

consumer = client.subscribe(topicName, subscriptionName, consumer_type=ConsumerType.Shared, receiver_queue_size=10, initial_position=InitialPosition.Earliest,schema=schema.StringSchema())

print (subscriptionName)

while True:
    msg = consumer.receive()
    print ("anything?")
    try:
        print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
        # Acknowledge successful processing of the message
        consumer.acknowledge(msg)
    except:
        # Message failed to be processed
        consumer.negative_acknowledge(msg)

client.close()
