# https://github.com/streamnative/developing_pulsar_exercise/blob/master/3_Streaming/solutions/java/src/main/java/sn_training/ex2_key_shared/CustomRoutingProducer.java
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
key = 0
currentMessage = ""
currentProducer = "customroutingproducer"
topic = "custom-routing"

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer(topic=topic, schema=schema.StringSchema(), send_timeout_millis=10000,block_if_queue_full=True,batching_type=BatchingType.KeyBased,message_routing_mode=PartitionsRoutingMode.RoundRobinDistribution,properties={"producer-name": currentProducer,"producer-id": currentProducer})

# str(datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S'))))
#batching_max_messages=1000, batching_max_allowed_size_in_bytes=131072, batching_max_publish_delay_ms=10, message_routing_mode=_pulsar.PartitionsRoutingMode.RoundRobinDistribution, 
# batching_type=_pulsar.BatchingType.Default,

while True:
    try:
        msgNum = msgNum + 1
        key = str(msgNum % 15)
        currentMessage = str(msgNum)
        print("Sending [{}] message [{}] at [{}]".format(str(key), str(currentMessage), str(datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S'))))
        producer.send_async(currentMessage,partition_key=key, callback=callback)


        # No Custom Partitioning in Python
        #                    .messageRoutingMode(MessageRoutingMode.CustomPartition)
        #            .messageRouter(new CustomMessageRouter())
        #.batcherBuilder(BatcherBuilder.KEY_BASED)

        time.sleep(1)

    except Exception as e: print(e)

client.close()
