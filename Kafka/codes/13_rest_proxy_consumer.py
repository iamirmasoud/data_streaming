import asyncio
import json
import random
from dataclasses import asdict, dataclass, field

import requests
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from faker import Faker

faker = Faker()
REST_PROXY_URL = "http://localhost:38082"
TOPIC_NAME = "com.sefidian.avro_consumer.click_events"
CONSUMER_GROUP = f"solution7-consumer-group-{random.randint(0,10000)}"


async def consume():
    """Consumes from REST Proxy"""
    # TODO: Define a consumer name
    consumer_name = ""
    # TODO: Define the appropriate headers
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#content-types
    headers = {}
    # TODO: Define the consumer group creation payload, use avro
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#post--consumers-(string-group_name)
    data = {}
    resp = requests.post(
        f"{REST_PROXY_URL}/consumers/{CONSUMER_GROUP}",
        data=json.dumps(data),
        headers=headers,
    )
    try:
        resp.raise_for_status()
    except:
        print(
            f"Failed to create REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
        )
        return
    print("REST Proxy consumer group created")

    resp_data = resp.json()
    #
    # TODO: Create the subscription payload
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#consumers
    #
    data = {}
    resp = requests.post(
        f"{resp_data['base_uri']}/subscription", data=json.dumps(data), headers=headers
    )
    try:
        resp.raise_for_status()
    except:
        print(
            f"Failed to subscribe REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
        )
        return
    print("REST Proxy consumer subscription created")
    while True:
        #
        # TODO: Set the Accept header to the same data type as the consumer was created with
        #       See: https://docs.confluent.io/current/kafka-rest/api.html#get--consumers-(string-group_name)-instances-(string-instance)-records
        #
        headers = {}
        resp = requests.get(f"{resp_data['base_uri']}/records", headers=headers)
        try:
            resp.raise_for_status()
        except:
            print(
                f"Failed to fetch records with REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
            )
            return
        print("Consumed records via REST Proxy:")
        print(f"{json.dumps(resp.json())}")
        await asyncio.sleep(0.1)


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))

    schema = avro.loads(
        """{
        "type": "record",
        "name": "click_event",
        "namespace": "com.sefidian",
        "fields": [
            {"name": "email", "type": "string"},
            {"name": "timestamp", "type": "string"},
            {"name": "uri", "type": "string"},
            {"name": "number", "type": "int"}
        ]
    }"""
    )


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = AvroProducer(
        {
            "bootstrap.servers": "PLAINTEXT://localhost:29092",
            "schema.registry.url": "http://localhost:8089",
        }
    )
    try:
        while True:
            p.produce(
                topic=topic_name,
                value=asdict(ClickEvent()),
                value_schema=ClickEvent.schema,
            )
            await asyncio.sleep(0.1)
    except Exception as e:
        print(e)
        raise


async def produce_consume(topic_name):
    """Runs the Producer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume())
    await t1
    await t2


def main():
    """Runs the simulation against REST Proxy"""
    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()
