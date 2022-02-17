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

msgNum = 0
currentMessage = ""
topic = "delayed-message"

def current_milli_time():
    return round(time.time() * 1000)

def callback(res, msg_id):
    print('Message published: %s' % res)

# Async left to developer


client = pulsar.Client('pulsar://localhost:6650')

# https://github.com/streamnative/developing_pulsar_exercise/blob/master/2_Messaging/solutions/java/src/main/java/sn_training/ex2_scheduled/ScheduledMessageProducer.java

while True:
    try:
        msgNum = msgNum + 1
        ts = current_milli_time()
        deliveryTime = ts+10000
        currentMessage = "Message number:" + str(msgNum) + ", sent at ts: " + str(ts);
        currentProducer = "producerScheduled"
        print("Sending message [{}] at delivery time [{}]".format(currentMessage, deliveryTime))
        producer = client.create_producer(topic=topic, schema=schema.StringSchema(), properties={"producer-name": currentProducer,"producer-id": currentProducer})
        producer.send(currentMessage, deliver_at=deliveryTime)

# : Specify the this message should not be delivered earlier than the specified timestamp. The timestamp is milliseconds and based on UTC
        time.sleep(2)

    except Exception as e: print(e)

client.close()
