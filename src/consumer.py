import io
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField, StringDeserializer,
)
from src.kafka_setting import (
    KAFKA_TOPIC_NAME,
    PRODUCT_AVRO_SCHEMA,
    SNOWFLAKE_ROLE,
    BATCH_TIME_LIMIT_SECONDS,
    kafka_config, CONSUMER_GROUP_ID, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ACCOUNT, SNOWFLAKE_SCHEMA, SNOWFLAKE_DATABASE,
    SNOWFLAKE_PASSWORD, SNOWFLAKE_STAGE_NAME, SNOWFLAKE_USERNAME, SNOWFLAKE_TABLE_NAME, BATCH_SIZE
)
from confluent_kafka.schema_registry.avro import AvroDeserializer

from src.schema import KafkaSchema
import snowflake.connector
import os


# Import the running flag and signal handler from utils


class KafkaConsumer:
    def __init__(self, group_id: str = CONSUMER_GROUP_ID):
        self.schema_registry_client = KafkaSchema()
        self.avro_deserializer = AvroDeserializer(self.schema_registry_client.schema_reg_client, PRODUCT_AVRO_SCHEMA)
        self.string_deserializer = StringDeserializer("utf-8")
        consumer_config = {
            **kafka_config,  # Create a copy and spread existing config
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
            "enable.auto.commit": False  # Ensure this is explicitly False for manual commits
        }
        self.snowflake_connection = None
        self._initiate_snowflake_connection()
        self.consumer = Consumer(consumer_config)

        # batch variable
        self.current_batch = []
        self.last_batch_time = time.time()
        print(f"Consumer starting with group ID: {CONSUMER_GROUP_ID}")

    def _initiate_snowflake_connection(self):
        try:
            self.snowflake_connection = snowflake.connector.connect(
                user=SNOWFLAKE_USERNAME,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                role=SNOWFLAKE_ROLE
            )
            print("Successfully connected to Snowflake.")
        except Exception as e:
            print(f"ERROR: Could not connect to Snowflake: {e}", file=sys.stderr)
            sys.exit(1)

    def _load_batch_to_snowflake(self):
        """
        Loads the accumulated batch of messages into Snowflake using a stage and COPY INTO.
        """
        if not self.current_batch:
            return
        temp_file_path = None
        batch_size = len(self.current_batch)
        print(f"Loading batch of {batch_size} records to Snowflake...")
        try:
            # Prepare data as JSON Lines in an in -memory file
            json_data = io.StringIO()
            for record in self.current_batch:
                processed_record = record.copy()
                if isinstance(processed_record['updated_timestamp'], datetime):
                    # Convert datetime object to ISO 8601 string
                    processed_record['updated_timestamp'] = processed_record['updated_timestamp'].astimezone(
                        timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                elif 'updated_timestamp' in processed_record and isinstance(processed_record['updated_timestamp'], int):
                    # Convert Avro timestamp-millis (long) to ISO 8601 string for Snowflake JSON ingestion
                    # You might need to adjust this formatting based on your Snowflake column type and format
                    processed_record['updated_timestamp'] = (
                        datetime.fromtimestamp(processed_record['updated_timestamp'] / 1000, tz=timezone.utc)
                        .isoformat(timespec='milliseconds')
                        .replace('+00:00', 'Z')
                    )

                json_data.write(json.dumps(processed_record) + '\n')
            json_data.seek(0)
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8', suffix=".json") as temp_file:
                temp_file.write(json_data.getvalue())
                temp_file_path = temp_file.name
            print(f"  [DEBUG] Temporary file created: {temp_file_path}")
            if not os.path.exists(temp_file_path):
                print(f"  [ERROR] Temp file DOES NOT EXIST locally: {temp_file_path}", file=sys.stderr)
                raise FileNotFoundError(f"Local temporary file not found for Snowflake PUT: {temp_file_path}")

                # Convert Windows backslashes to forward slashes for Snowflake PUT command
            snowflake_compatible_path = temp_file_path.replace(os.sep, '/')
            # Generate a unique file name for the stage
            file_name = f"product_batch_{str(uuid.uuid4())}.json"
            stage_path = f"@{SNOWFLAKE_STAGE_NAME}/kafka_ingestion/{file_name}"
            print("[DEBUG] SNOWFLAKE STAGE PATH", stage_path)
            # Upload the in-memory file to Snowflake stage
            cursor = self.snowflake_connection.cursor()

            try:
                print(
                    f"  [DEBUG] Initiating PUT operation for {file_name} from local path: {snowflake_compatible_path}...")
                put_sql = f"PUT 'file://{snowflake_compatible_path}' '{stage_path}' AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                cursor.execute(put_sql)
                print(f"Uploaded {file_name} to {SNOWFLAKE_STAGE_NAME}/kafka_ingestion/")

                # Copy data from stage into the target table
                copy_sql = f"""
                            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE_NAME}
                            FROM {stage_path}
                            FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)
                            MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
                            ON_ERROR = 'ABORT_STATEMENT'
                            ;
                            """
                cursor.execute(copy_sql)
                self.snowflake_connection.commit()
                self.current_batch = []
                print(f"Successfully loaded {batch_size} records into Snowflake.")
            except snowflake.connector.errors.ProgrammingError as e:
                print(f"Snowflake SQL Error: {e}", file=sys.stderr)
        except FileNotFoundError as e:
            print(f"ERROR: Local file system issue: {e}", file=sys.stderr)
        except Exception as e:
            print(f"ERROR during batch loading to Snowflake: {e}", file=sys.stderr)
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                print(f"  [DEBUG] Deleting temporary local file: {temp_file_path}")
                os.remove(temp_file_path)
            self.current_batch = []  # Clear the batch regardless of success

    def consume_message(self):
        try:
            self.consumer.subscribe([KAFKA_TOPIC_NAME])
            print(f"Subscribed to topic: {KAFKA_TOPIC_NAME}")
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    # Check if time limit for current batch is reached
                    if self.current_batch and (time.time() - self.last_batch_time >= BATCH_TIME_LIMIT_SECONDS):
                        self._load_batch_to_snowflake()
                        self.last_batch_time = time.time()  # Reset timer
                        self.consumer.commit(asynchronous=False)
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event, not an error
                        print(f"Reached end of partition {msg.partition()} for topic {msg.topic()}")
                        continue
                    elif msg.error():
                        # Other Kafka errors
                        print(f"Consumer error: {msg.error()}", file=sys.stderr)
                        if msg.error().code() == KafkaError.AUTHENTICATION_FAILED:
                            print("  Authentication failed. Stopping consumer.", file=sys.stderr)
                        continue
                try:
                    # Deserialize message key and value
                    # For value, context is important for AvroDeserializer to determine subject
                    deserialized_key = self.string_deserializer(msg.key())
                    deserialized_value = self.avro_deserializer(
                        msg.value(),
                        SerializationContext(msg.topic(), MessageField.VALUE)
                    )

                    if deserialized_key is not None and deserialized_value is not None:
                        # Append deserialized record to current batch
                        self.current_batch.append(deserialized_value)
                        # Commit offset for the *last message of the batch* later,
                        # after successful load to Snowflake. For now, we accumulate.
                        # Offsets will be committed only when a batch successfully lands in Snowflake.

                        # Check if batch size or time limit is reached
                        if len(self.current_batch) >= BATCH_SIZE:
                            print(f"Batch size {BATCH_SIZE} reached.")
                            self._load_batch_to_snowflake()
                            # If batch loaded successfully, commit offsets up to this point
                            self.consumer.commit(asynchronous=False)  # Synchronous commit for reliability
                            self.last_batch_time = time.time()  # Reset timer
                        elif time.time() - self.last_batch_time >= BATCH_TIME_LIMIT_SECONDS:
                            print(f"Batch time limit {BATCH_TIME_LIMIT_SECONDS}s reached.")
                            self._load_batch_to_snowflake()
                            # If batch loaded successfully, commit offsets
                            self.consumer.commit(asynchronous=False)
                            self.last_batch_time = time.time()  # Reset timer
                except Exception as e:
                    print(f"Error during message deserialization or processing: {e}", file=sys.stderr)
        except KeyboardInterrupt:
            print("consumer stopped")
        finally:
            if self.current_batch:
                print("Flushing final batch to Snowflake...")
                self._load_batch_to_snowflake()
                self.consumer.commit(asynchronous=False)  # Final commit

            print("Closing consumer and Snowflake connection...")
            if self.snowflake_connection:
                self.snowflake_connection.close()
            self.consumer.close()
            print("Consumer and Snowflake connection closed.")


if __name__ == "__main__":
    consumer = KafkaConsumer()
    consumer.consume_message()
