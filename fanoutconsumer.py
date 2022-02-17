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
# Test with command line bin/pulsar-client consume --regex 'persistent://public/default/dynamic-topic.*' -s all -n 0
# Needs .*

subscriptionName = 'fanout-sub-' + str(int(time.time()))

client = pulsar.Client('pulsar://localhost:6650')

consumer = client.subscribe(re.compile('persistent://public/default/dynamic-topic.*'), subscriptionName, consumer_type=ConsumerType.Exclusive, initial_position=InitialPosition.Earliest,schema=schema.StringSchema())

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
