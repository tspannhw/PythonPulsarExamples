# https://github.com/streamnative/developing_pulsar_exercise/blob/master/3_Streaming/solutions/java/src/main/java/sn_training/ex4_reader/SamplingReader.java
# This needs some work
# timeout checks, most async
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
from pulsar import ConsumerType, InitialPosition, MessageId
import asyncio

topicName = "stream-sample"
runningTotal = 0
lastMessageId = None

def current_milli_time():
	return round(time.time() * 1000)

def my_listener(reader, message):
	global runningTotal
	global lastMessageId

	runningTotal = runningTotal + 1
	print("Reader: message='{}' id='{}' at [{}] [{}] [{}]".format(message.data(), message.message_id(), str(round(time.time())), datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S'), str(message.publish_timestamp())))

def work_loop():
	global client 
	global reader
	global lastMessageId

	print("work_loop")
	message = reader.read_next(timeout_millis=5000)

	if ((lastMessageId != None) and (lastMessageId == message.message_id()) ):
		print("Message id didn't change, Resetting back to end of topic")
		reader.seek(MessageId.latest)
		# seekAsync missing
	else:
		reader.seek(message.publish_timestamp() - 1000)
		# seekAsync missing
		lastMessageId = message.message_id()
		print("Sample message at " + str(message.publish_timestamp()) + " with value " + str(message.data()))

async def work():
	while True:

		work_loop()
		await asyncio.sleep(5)
		print("Waiting for messages...")

### Main

client = pulsar.Client('pulsar://localhost:6650')

# https://pulsar.apache.org/api/python/#pulsar.Client.create_reader
# ,reader_listener = my_listener 
reader = client.create_reader(topicName, MessageId.latest, schema=schema.StringSchema())

loop = asyncio.get_event_loop()

try:
	asyncio.ensure_future(work())
	loop.run_forever()
except KeyboardInterrupt:
	pass
finally:
	print("Shutdown")
	loop.close()
