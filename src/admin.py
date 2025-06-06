import sys

from confluent_kafka.admin import AdminClient, NewTopic
from kafka_setting import kafka_config


class KafkaAdminSetting:
    def __init__(self):
        self.admin = AdminClient(kafka_config)

    def check_topic_existence(self, topic_name: str):
        """
        Returns true if topic exists otherwise False
        """
        topic_metadata = self.admin.list_topics(timeout=5)
        if topic_name in topic_metadata.topics:
            print(f"⚠️ Topic '{topic_name}' already exists.")
            return True
        else:
            return False

    def create_new_topic(self, topic_name: str, num_partitions: int = 4, replication_factor: int = 5):
        """
        Creates a new topic
        """
        new_topic = NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        futures = self.admin.create_topics([new_topic])
        try:
            futures[topic_name].result()
            print(f"✅ Created topic '{topic_name}'")
        except Exception as e:
            print(f"❌ Failed to create topic '{topic_name}': {e}", file=sys.stderr)
            raise
