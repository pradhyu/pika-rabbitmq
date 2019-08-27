# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import logging
import pika
import json

logging.basicConfig(level=logging.ERROR)

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', credentials=credentials, port=5672)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.exchange_declare(
    exchange="test_exchange",
    exchange_type="topic",
    passive=False,
    durable=True,
    auto_delete=False)
with open('../tests/test_01.json') as f:
    payload = json.load(f)
    f.close()

print("Sending json message to a queue")
channel.basic_publish(
    exchange='test_exchange',
    routing_key='standard_key',
    body=json.dumps(payload),
    properties=pika.BasicProperties(content_type='application/json', delivery_mode=1))

connection.sleep(5)

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
