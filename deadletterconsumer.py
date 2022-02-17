# https://github.com/streamnative/developing_pulsar_exercise/blob/master/2_Messaging/solutions/java/src/main/java/sn_training/ex3_flaky/DeadLetterConsumer.java

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

totalInDeadLetter = 0
currentMessage = ""
topicName = "flaky-DLQ"
subscriptionName = "dql"

def current_milli_time():
    return round(time.time() * 1000)

def callback(res, msg_id):
    print('Message published: %s' % res)

def my_listener(consumer, message):
    # process message
    print(message)
    consumer.acknowledge(message)

client = pulsar.Client('pulsar://localhost:6650')

consumer = client.subscribe(topicName, subscriptionName, initial_position=InitialPosition.Latest,schema=schema.StringSchema())

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
