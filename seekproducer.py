# https://github.com/streamnative/developing_pulsar_exercise/blob/master/3_Streaming/solutions/java/src/main/java/sn_training/ex3_seek/SeekProducer.java
# https://docs.google.com/spreadsheets/d/1YHYTkIXR8-Ql103u-IMI18TXLlGStK8uJjDsOOA0T20/edit#gid=1031448742
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
from pulsar import ConsumerType, InitialPosition, BatchingType,PartitionsRoutingMode

def callback(res, msg_id):
    print("Message published: {} {}".format(str(res), str(msg_id)))

msgNum = 0
currentMessage = ""
currentProducer = "seekproducer"
topic = "seeking"

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer(topic=topic, schema=schema.StringSchema(), send_timeout_millis=10000,block_if_queue_full=True,properties={"producer-name": currentProducer,"producer-id": currentProducer})

while True:
    try:
        msgNum = msgNum + 1
        v = random.randint(0, 10)
        if ( v == 0 ):
            currentMessage = "RESET"   # Seek Message
        else:
            currentMessage = str(v)

        print("Sending message [{}] at [{}]".format(str(currentMessage), str(datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S'))))
        producer.send_async(currentMessage,callback=callback)

        time.sleep(1)

    except Exception as e: print(e)

client.close()
