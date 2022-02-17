# https://github.com/streamnative/developing_pulsar_exercise/blob/master/3_Streaming/solutions/java/src/main/java/sn_training/ex3_seek/RewindingConsumer.java

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
topicName = "seeking"
subscriptionName = "my-subscription"
runningTotal = 0
lastReset = None
newReset = None
isReset = False 

def current_milli_time():
    return round(time.time() * 1000)

def seekToLastReset(consumer, message):
    global lastReset
    global newReset
    global runningTotal
    global isReset

    if (lastReset != newReset):
        print("First total is " + str(runningTotal) + ", resetting")
        
        resetPos = lastReset;
        
        lastReset = newReset;
        
        runningTotal = 0;

        # No consumer.seekAsync(resetPos); for Python
        consumer.seek(resetPos)
    else:
        print("Second total is " + str(runningTotal) + ", continuing")
        runningTotal = 0
        consumer.acknowledge_cumulative(message)
        #return consumer.acknowledgeCumulativeAsync(newReset)

def my_listener(consumer, message):
    global lastReset
    global newReset
    global runningTotal
    global isReset

    print("Consumer: message='{}' id='{}' at [{}] [{}] [{}]".format(message.data(), message.message_id(), str(round(time.time())), datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S'), str(message.publish_timestamp())))

    if (lastReset == None):
        lastReset = message.message_id()
   
    if ( "RESET" in str(message.data())): 
    	print("Reset in my listener")
        isReset = True
    else:
        runningTotal += int(message.data())
        isReset = False

    if (isReset):
        if (lastReset != newReset):
            print("First total is " + str(runningTotal) + ", resetting")
            resetPos = lastReset
            lastReset = newReset
            runningTotal = 0
            # No consumer.seekAsync(resetPos); for Python
            consumer.seek(resetPos)
        else:
            print("Second total is " + str(runningTotal) + ", continuing")
            runningTotal = 0
            consumer.acknowledge_cumulative(message)
    else:
        consumer.acknowledge_cumulative(message)
    
async def work():
    while True:
        await asyncio.sleep(60)
        print("Waiting for messages...")

client = pulsar.Client('pulsar://localhost:6650')

consumer = client.subscribe(topicName, subscriptionName, consumer_type=ConsumerType.Failover, message_listener=my_listener, initial_position=InitialPosition.Earliest,schema=schema.StringSchema())

loop = asyncio.get_event_loop()

try:
    asyncio.ensure_future(work())
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    print("Shutdown")
    loop.close()
