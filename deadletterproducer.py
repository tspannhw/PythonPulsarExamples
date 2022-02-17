# https://github.com/streamnative/developing_pulsar_exercise/blob/master/2_Messaging/solutions/java/src/main/java/sn_training/ex3_flaky/DeadLetterProducer.java

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
topic = "flaky"
currentProducer = "deadletterproducer"

client = pulsar.Client('pulsar://localhost:6650')

while True:
    try:
        msgNum = msgNum + 1

        if (msgNum % 5):
            currentMessage = "FAIL " + str(msgNum)
            print("Sending FAIL message [{}] at [{}]".format(currentMessage, str(datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S'))))
        else:
            currentMessage = str(msgNum) 
            print("Sending message [{}] at [{}]".format(currentMessage, str(datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S'))))

        producer = client.create_producer(topic=topic, schema=schema.StringSchema(), properties={"producer-name": currentProducer,"producer-id": currentProducer})
        producer.send(currentMessage)

        time.sleep(1)

    except Exception as e: print(e)

client.close()
