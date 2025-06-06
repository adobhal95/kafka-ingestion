import sys
import time
from confluent_kafka import Producer, KafkaException, KafkaError
from kafka_setting import kafka_config, KAFKA_TOPIC_NAME, KAFKA_SCHEMA_NAME, PRODUCT_AVRO_SCHEMA
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry.avro import AvroSerializer
from src.admin import KafkaAdminSetting
from src.db import DataBaseConnection
from src.schema import KafkaSchema
from src.utils import get_last_successful_timestamp, delivery_callback, set_last_successful_timestamp
from datetime import datetime
from decimal import Decimal


class KafkaProducerApp:
    def __init__(self):
        self.producer = Producer(kafka_config)
        self.database_connection = DataBaseConnection()
        self.schema_manager = KafkaSchema()
        admin = KafkaAdminSetting()
        if admin.check_topic_existence(KAFKA_TOPIC_NAME):
            print("Topic exists....")
        else:
            print(f"creating topic:{KAFKA_TOPIC_NAME}")
            admin.create_new_topic(KAFKA_TOPIC_NAME, num_partitions=10, replication_factor=3)
        if self.schema_manager.check_schema_existence(KAFKA_SCHEMA_NAME):
            print("Schema exists....")
        else:
            print(f"Creating new schema:{KAFKA_SCHEMA_NAME}")
            self.schema_manager.create_schema(KAFKA_SCHEMA_NAME)
        self.avro_serializer = AvroSerializer(schema_registry_client=self.schema_manager.schema_reg_client,
                                              schema_str=PRODUCT_AVRO_SCHEMA)
        self.string_serializer = StringSerializer("utf-8")
        self.last_successful_read_timestamp = get_last_successful_timestamp()
        print(f"Producer starting with high-watermark: {self.last_successful_read_timestamp}")

    def produce_message(
            self,
            topic_name: str,
    ):
        """
        Fetches incremental data from PostgreSQL and produces Avro-serialized messages to Kafka.
        """
        self.producer.poll(0.0)
        try:
            # Query PostgreSQL for records newer than the last successful read timestamp
            # PostgreSQL can compare datetime objects directly.
            query = "SELECT product_id, name, category, price, updated_timestamp FROM products WHERE updated_timestamp > %s ORDER BY updated_timestamp ASC;"
            products = self.database_connection.fetch_query_all(query, (self.last_successful_read_timestamp,))
            if not products:
                print("...No new data to load...")
                time.sleep(10)
                return
            print(f"Found {len(products)} new/updated records since {self.last_successful_read_timestamp}")
            current_max_timestamp = self.last_successful_read_timestamp
            for product_data in products:
                # Convert datetime object to milliseconds since epoch for Avro 'timestamp-millis'
                if isinstance(product_data['updated_timestamp'], datetime):
                    product_data['updated_timestamp'] = int(
                        product_data['updated_timestamp'].timestamp() * 1000)  # Store original for high-watermark
                # Convert Decimal type (from PostgreSQL NUMERIC) to float for Avro 'double'
                if isinstance(product_data['price'], Decimal):
                    product_data['price'] = float(product_data['price'])
                avro_value = self.avro_serializer(
                    product_data,
                    SerializationContext(topic_name, MessageField.VALUE),
                )
                if avro_value is None:
                    print(f"WARNING: Failed to serialize record: {product_data}. Skipping.", file=sys.stderr)
                    continue
                try:
                    self.producer.produce(
                        topic=topic_name,
                        key=self.string_serializer(str(product_data["product_id"])),
                        value=avro_value,
                        on_delivery=delivery_callback,
                    )
                except BufferError:
                    print("Producer queue full. Flushing and retrying...", file=sys.stderr)
                    self.producer.flush(timeout=10)
                    self.producer.poll(0)
                    self.producer.produce(
                        topic=topic_name,
                        key=self.string_serializer(str(product_data["product_id"])),
                        value=avro_value,
                        on_delivery=delivery_callback,
                    )
                except KafkaException as e:
                    kafka_error = e.args[0]
                    print(
                        f"ERROR: Producer error for key {product_data.get('product_id', 'N/A')}: {kafka_error.str()} (Code: {kafka_error.code()})",
                        file=sys.stderr)
                    if kafka_error.code() == KafkaError.AUTHENTICATION_FAILED:
                        print("  Authentication failed. Stopping producer.", file=sys.stderr)
                        sys.exit(1)
                except Exception as e:
                    print(
                        f"ERROR: Unexpected error during data processing for record {product_data.get('product_id', 'N/A')}: {e}",
                        file=sys.stderr)
            query = "SELECT max(updated_timestamp) as updated_timestamp FROM products;"
            current_max_timestamp = self.database_connection.fetch_query_once(query)["updated_timestamp"]
            set_last_successful_timestamp(current_max_timestamp)
            self.last_successful_read_timestamp = current_max_timestamp
            print("Current Time Update", self.last_successful_read_timestamp)
        except Exception as e:
            print("Error while producing message:", e, file=sys.stderr)
        finally:
            remaining_messages = self.producer.flush(timeout=30)
            if remaining_messages > 0:
                print(f"WARNING: {remaining_messages} messages still in queue after flush timeout.", file=sys.stderr)

    def run_producer(self):
        try:
            while True:
                self.produce_message(KAFKA_TOPIC_NAME)
                time.sleep(5)  # poll for updates every 5 seconds
        except KeyboardInterrupt:
            print("Producer stopped by user.")
        except Exception as e:
            print("Error happened in main loop:", e)
        finally:
            self.database_connection.close()
            print("Producer stopped....")


if __name__ == "__main__":
    app = KafkaProducerApp()
    app.run_producer()
