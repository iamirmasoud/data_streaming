import asyncio
import json
import random
from dataclasses import dataclass, field

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker

faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:29092"
TOPIC_NAME = "com.sefidian.clickevents"


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    # uri: str = field(default_factory=faker.uri)
    uri: str = field(default_factory=lambda: random.choice(["A", "B", "C", "D", "E"]))
    number: int = field(default_factory=lambda: random.randint(0, 999))

    def serialize(self):
        """Serializes the object in JSON string format"""
        # TODO: Serializer the Purchase object
        #       See: https://docs.python.org/3/library/json.html#json.dumps
        return json.dumps(
            {
                "email": self.email,
                "timestamp": self.timestamp,
                "uri": self.uri,
                "number": self.number,
            }
        )


async def produce_sync(topic_name):
    """Produces data synchronously into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    # TODO: Write a synchronous production loop.
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.flush
    while True:
        # TODO: Instantiate a `Purchase` on every iteration. Make sure to serialize it before
        #       sending it to Kafka!
        event = ClickEvent()
        p.produce(topic_name, event.serialize())
        print(f"Produced {event}")
        p.flush()
        # Do not delete this!
        await asyncio.sleep(1)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    create_topic(TOPIC_NAME)
    try:
        asyncio.run(produce())
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce():
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce_sync(TOPIC_NAME))
    await t1


def create_topic(client):
    """Creates the topic with the given topic name"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    futures = client.create_topics(
        [NewTopic(topic=TOPIC_NAME, num_partitions=5, replication_factor=1)]
    )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            print("exiting production loop")


if __name__ == "__main__":
    main()

# Check the new topic
# kafkacat -C -b localhost:29092 -o -5 -t com.sefidian.clickevents.popular
