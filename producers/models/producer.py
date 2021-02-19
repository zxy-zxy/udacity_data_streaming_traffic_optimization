"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL,
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self):
        kafka_client = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})

        already_existing_topics = kafka_client.list_topics().topics.keys()
        if self.topic_name in already_existing_topics:
            logger.info(f"Topic {self.topic_name} already exists.")
            return

        new_topic = NewTopic(
            topic=self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas,
        )

        futures = kafka_client.create_topics([new_topic])
        for _, futures in futures.items():
            try:
                futures.result()
                logger.info(f"Topic {self.topic_name} successfully created.")
            except Exception as e:
                logger.error(f"Cannot create topic {self.topic_name}.")
                logger.error(f"Error text: {e}.")

    def close(self):
        if self.producer:
            logger.debug(f"Closing producer {self.topic_name}.")
            self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
