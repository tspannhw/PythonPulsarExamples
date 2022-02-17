# https://github.com/streamnative/developing_pulsar_exercise/blob/master/3_Streaming/solutions/java/src/main/java/sn_training/ex2_key_shared/KeySharedConsumer.java
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
import asyncio

# There is no subscribeAsync for python3

currentMessage = ""
topicName = "custom-routing"
subscriptionName = "key-shared-sub"

def current_milli_time():
    return round(time.time() * 1000)

def my_listener(consumer, message):
    # process message
    # print(message)
    print("Key Shared Consumer: key='{}'' message='{}' id='{}' at [{}] [{}] [{}]".format(message.partition_key(), message.data(), message.message_id(), str(round(time.time())), datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S'), str(message.publish_timestamp())))
    consumer.acknowledge(message)

async def work():
    while True:
        await asyncio.sleep(60)
        print("Waiting for messages...")

client = pulsar.Client('pulsar://localhost:6650')

consumer = client.subscribe(topicName, subscriptionName, consumer_type=ConsumerType.KeyShared, message_listener=my_listener, initial_position=InitialPosition.Earliest,schema=schema.StringSchema())

loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(work())
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    print("Shutdown")
    loop.close()
    client.close()
