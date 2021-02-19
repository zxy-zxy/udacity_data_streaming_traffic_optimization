"""Defines core consumer functionality"""
import logging

from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer, SerializerError
from tornado import gen

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "group.id": topic_name_pattern,
        }

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING

        consumer.assign(partitions)
        logger.info("partitions assigned for %s", self.topic_name_pattern)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        try:
            message = self.consumer.poll(1.0)
        except SerializerError as e:
            logger.error(f"Error occurred while consuming data: {e}.")
            return 1

        if message is None:
            logger.debug("Message is None.")
            return 0

        if message.error():
            logger.error(f"Message contains an error: {message.error()}.")
            return 1

        logger.debug(f"Message is {message.value()}.")
        self.message_handler(message)
        return 1

    def close(self):
        self.consumer.close()
