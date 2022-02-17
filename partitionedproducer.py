# https://github.com/streamnative/developing_pulsar_exercise/blob/master/2_Messaging/solutions/java/src/main/java/sn_training/ex3_flaky/DeadLetterProducer.java
# https://pulsar.apache.org/api/python/#pulsar.Client
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

def callback(res, msg_id):
    print("Message published: {} {}".format(str(res), str(msg_id)))

msgNum = 0
key = 0
currentMessage = ""
currentProducer = "partitionedProducer"
topic = "ex1-basic"

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer(topic=topic, schema=schema.StringSchema(), send_timeout_millis=10000,block_if_queue_full=True, properties={"producer-name": currentProducer,"producer-id": currentProducer})
# str(datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S'))))

while True:
    try:
        msgNum = msgNum + 1
        key = str(msgNum % 5)
        currentMessage = str(msgNum)
        print("Sending [{}] message [{}] at [{}]".format(str(key), str(currentMessage), str(datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S'))))
        producer.send_async(currentMessage,partition_key=key, callback=callback)

        time.sleep(1)

    except Exception as e: print(e)

client.close()
