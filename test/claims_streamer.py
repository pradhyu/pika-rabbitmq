# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import logging
import pika
import json

from pika import PlainCredentials, ConnectionParameters
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection

logging.basicConfig(level=logging.ERROR)


credentials: PlainCredentials = pika.PlainCredentials('guest', 'guest')
parameters: ConnectionParameters = pika.ConnectionParameters('localhost', credentials=credentials, port=5672)
connection: BlockingConnection = pika.BlockingConnection(parameters)
channel: BlockingChannel = connection.channel()
channel.exchange_declare(
    exchange="test_exchange",
    exchange_type="topic",
    passive=False,
    durable=True,
    auto_delete=False)

# open the test file as payload
with open('../tests/test_01.json') as f:
    payload = json.load(f)
    f.close()

print("Sending json message to a queue")
channel.basic_publish(
    exchange='test_exchange',
    routing_key='standard_key',
    body=json.dumps(payload), # need to dump the payload as body
    properties=pika.BasicProperties(content_type='application/json', delivery_mode=1))

connection.sleep(5)

# Get ten messages and break out
print("Consuming 1 messages from queue khai")
for method_frame, properties, body in channel.consume('khai'):
    # Display the message parts
    print(method_frame)
    print(properties)
    print(body)
    # Acknowledge the message
    channel.basic_ack(method_frame.delivery_tag)
    # Escape out of the loop after 1 messages
    if method_frame.delivery_tag == 1:
        break
# Cancel the consumer and return any pending messages
requeued_messages = channel.cancel()
print('Requeued %i messages' % requeued_messages)


print("Sending text message to group")
channel.basic_publish(
    'test_exchange', 'group_key', 'Message to group_key',
    pika.BasicProperties(content_type='text/plain', delivery_mode=1))

connection.sleep(5)

print("Sending text message")
channel.basic_publish(
    'test_exchange', 'standard_key', 'Message to standard_key',
    pika.BasicProperties(content_type='text/plain', delivery_mode=1))

connection.close()
