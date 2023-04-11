from typing import Dict
import json
import asyncio

from fastapi import FastAPI
import asyncpg
from confluent_kafka import Consumer, Producer, KafkaError

app = FastAPI()

DATABASE_CONFIG = {
    'user': 'your_username',
    'password': 'your_password',
    'database': 'your_database',
    'host': 'your_host'
}


async def handle_message(payload: Dict[str, str]):
    try:
        message_id = int(payload.get('id'))
        message_text = payload.get('text')
        conn = await asyncpg.connect(**DATABASE_CONFIG)
        await conn.execute('INSERT INTO "Message" (id, text) VALUES ($1, $2)', message_id, message_text)
        await conn.close()
    except Exception as e:
        print(f'Error while handling message: {e}')
        return


async def consume():
    consumer_conf = {
        'bootstrap.servers': 'your_kafka_bootstrap_servers',
        'group.id': 'your_consumer_group_id',
        'auto.offset.reset': 'earliest',
    }
    consumer = Consumer(consumer_conf)

    consumer.subscribe(['your_first_topic_name'])

    producer_conf = {
        'bootstrap.servers': 'your_kafka_bootstrap_servers',
    }
    producer = Producer(producer_conf)

    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                print(f'End of partition reached: {message.topic()} [{message.partition()}]')
            else:
                print(f'Error while consuming message: {message.error()}')
            continue

        try:
            payload = json.loads(message.value())
            asyncio.create_task(handle_message(payload))
            producer.produce('your_second_topic_name', value=message.value())
            producer.flush()
        except json.JSONDecodeError as e:
            print(f'Error while decoding message value: {e}')
            continue

    consumer.close()


@app.get('/')
async def read_root():
    return {'status': 'running'}


@app.on_event('startup')
async def startup_event():
    asyncio.create_task(consume())
