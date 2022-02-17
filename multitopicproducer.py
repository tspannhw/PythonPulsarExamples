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

# Reference https://pulsar.apache.org/api/python/2.9.0-SNAPSHOT/#pulsar.Producer
# Reference https://github.com/apache/pulsar/tree/master/pulsar-client-cpp/python/examples
# Reference https://github.com/streamnative/examples/tree/master/cloud/python
# This is complex for python https://raw.githubusercontent.com/streamnative/developing_pulsar_exercise/master/2_Messaging/solutions/java/src/main/java/sn_training/ex1_basics/MultiTopicProducer.java

counter = 0
currentTopic = ""
currentMessage = ""

def callback(res, msg_id):
    print('Message published: %s' % res)

# Async left to developer


client = pulsar.Client('pulsar://localhost:6650')

while True:
    try:
        counter = counter + 1
        currentTopic = "persistent://public/default/dynamic-topic-" + str(counter % 5);
        currentMessage = str(counter)
        currentProducer = "producer" + str(int(time.time()))
        print("Sending message [{}] to topic [{}]".format(currentMessage, currentTopic))
        producer = client.create_producer(topic=currentTopic, schema=schema.StringSchema(), properties={"producer-name": currentProducer,"producer-id": currentProducer})
        producer.send_async(currentMessage, callback=callback)

        time.sleep(2)

    except Exception as e: print(e)

client.close()
