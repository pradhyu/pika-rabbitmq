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

num_of_trials = 5
print("Sending json message to a queue")
for i in range(num_of_trials):
    channel.basic_publish(
        exchange='test_exchange',
        routing_key='standard_key',
        body=json.dumps(payload),  # need to dump the payload as body
        properties=pika.BasicProperties(content_type='application/json', delivery_mode=1))

connection.sleep(5)

# Get ten messages and break out
print(f'Consuming last {num_of_trials} messages from queue khai')
for method_frame, properties, body in channel.consume('khai'):
    # Display the message parts
    print(method_frame)
    print(properties)
    print(json.loads(body))
    # Acknowledge the message
    channel.basic_ack(method_frame.delivery_tag)
    # Escape out of the loop after num_of_trials messages
    if method_frame.delivery_tag == num_of_trials:
        break
# Cancel the consumer and return any pending messages
requeued_messages: int = channel.cancel()
print('Requeued %i messages=' % requeued_messages)

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
