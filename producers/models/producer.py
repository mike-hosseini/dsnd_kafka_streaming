"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

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

        self.broker_properties = {"bootstrap.servers": "PLAINTEXT://localhost:9092"}

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.schema_registry = CachedSchemaRegistryClient(
            {"url": "http://localhost:8081"}
        )

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
            schema_registry=self.schema_registry,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        client = AdminClient(self.broker_properties)

        topic_metadata = client.list_topics(timeout=5)

        existing_topics = set(t.topic for t in iter(topic_metadata.topics.values()))

        if self.topic_name not in existing_topics:
            futures = client.create_topics(
                [
                    NewTopic(
                        topic=self.topic_name,
                        num_partitions=self.num_partitions,
                        replication_factor=self.num_replicas,
                        config={
                            "cleanup.policy": "delete",
                            "compression.type": "lz4",
                            "delete.retention.ms": 2000,
                            "file.delete.delay.ms": 2000,
                            "retention.bytes": 536870912,  # 500 MB
                            "retention.ms": 604800000,  # 7 days
                        },
                    )
                ]
            )

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"topic {self.topic_name} created")
                except Exception as e:
                    logger.exception("topic creation failed", self.topic_name, e)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""

        if self.topic_name is not None:
            self.producer.flush()
            logger.info("all messages in the Producer queue have been delivered")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
