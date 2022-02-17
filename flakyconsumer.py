# https://github.com/streamnative/developing_pulsar_exercise/blob/master/2_Messaging/solutions/java/src/main/java/sn_training/ex3_flaky/DeadLetterConsumer.java
# https://pulsar.apache.org/api/python/#pulsar.Client.subscribe
import time
import pulsar
import sys
import datetime
import re
from time import gmtime, strftime
import random, string
from pulsar.schema import *
from time import sleep
import traceback
import random
from pulsar import ConsumerType, InitialPosition

totalReceived = 0
failureCount = 0
currentMessage = ""
topicName = "flaky"
deadLetterTopicName = "flaky-DLQ"
subscriptionName = "flaky-sub"
maxRedeliveryCount = 2

def current_milli_time():
    return round(time.time() * 1000)

def callback(res, msg_id):
    print('Message published: %s' % res)

def my_listener(consumer, message):
    # process message
    print(message)
    consumer.acknowledge(message)

client = pulsar.Client('pulsar://localhost:6650')


# Dead Letter Queue Not Supported in Python Yet
# https://github.com/apache/pulsar/issues/9741
# https://pulsar.apache.org/api/python/#pulsar.Consumer.redeliver_unacknowledged_messages
# Pulsar Feature Matrix https://docs.google.com/spreadsheets/d/1YHYTkIXR8-Ql103u-IMI18TXLlGStK8uJjDsOOA0T20/edit#gid=1784579914

 #               .deadLetterPolicy(DeadLetterPolicy.builder()
 #                       .maxRedeliverCount(maxRedeliveryCount)
 #                       .deadLetterTopic(deadLetterTopic)

consumer = client.subscribe(topicName, subscriptionName, consumer_type=ConsumerType.Shared, initial_position=InitialPosition.Latest,schema=schema.StringSchema(),negative_ack_redelivery_delay_ms=5000)

print (subscriptionName)

while True:
    msg = consumer.receive()
    print ("anything?")
    try:
        print("Dead Letter Consumer message '{}' id='{}' at {} {}".format(msg.data(), msg.message_id(), str(round(time.time())), datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')))
        # Acknowledge successful processing of the message
        consumer.acknowledge(msg)
    except:
        # Message failed to be processed
        consumer.negative_acknowledge(msg)

client.close()
