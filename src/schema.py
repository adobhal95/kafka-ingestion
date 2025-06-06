import sys

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema, SchemaRegistryError
from kafka_setting import schema_reg_conf, PRODUCT_AVRO_SCHEMA


class KafkaSchema:
    def __init__(self):
        self.schema_reg_client = SchemaRegistryClient(schema_reg_conf)

    def check_schema_existence(self, schema_name: str):
        """
        Returns true if schema exists in cluster
        """
        subjects = self.schema_reg_client.get_subjects()
        for schema_n in iter(subjects):
            if schema_n == schema_name:
                return True
        else:
            return False

    def create_schema(self, schema_name: str, schema_str: str = PRODUCT_AVRO_SCHEMA):
        """
        create a new schema for a topic
        """

        try:
            schema = Schema(schema_str=schema_str, schema_type="AVRO")
            schema_id: int = self.schema_reg_client.register_schema(schema_name, schema)
            print("=" * 10)
            print("⚠️ new schema created:", schema_id)
            print("=" * 10)
        except SchemaRegistryError as e:
            if e.http_status_code == 409:  # Conflict: schema already exists
                print(f"⚠️ Schema '{schema_name}' already exists. Skipping registration.")
            else:
                print(f"❌ Error registering schema: {e}", file=sys.stderr)
                raise
        except Exception as e:
            print("Error occurred: ", e)
            raise
