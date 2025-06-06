from config import env_setting

kafka_config = {
    "bootstrap.servers": env_setting.KAFKA_SERVER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": env_setting.KAFKA_USERNAME,
    "sasl.password": env_setting.KAFKA_PASSWORD,
}

schema_reg_conf = {
    "url": env_setting.SCHEMA_SERVER,
    "basic.auth.user.info": "{}:{}".format(
        env_setting.SCHEMA_USERNAME,
        env_setting.SCHEMA_PASSWORD,
    ),
    "timeout": 5000,
}

DATABASE_URL = f""

# Kafka Topic Name
KAFKA_TOPIC_NAME = 'product_updates'

# Avro Schema Definition (JSON string)
# This schema defines the structure of messages being sent to Kafka
PRODUCT_AVRO_SCHEMA = """
{
  "type": "record",
  "name": "Product",
  "namespace": "com.buyonline.products",
  "fields": [
    {"name": "product_id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "category", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "updated_timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""

KAFKA_SCHEMA_NAME = "product_updates-value"

# Consumer Group ID
CONSUMER_GROUP_ID = 'product_analytics_group_v3'

# Output directory for JSON files
OUTPUT_DIR = 'consumer_output'

# snowflake setting
SNOWFLAKE_USERNAME = env_setting.SNOWFLAKE_USER
SNOWFLAKE_PASSWORD = env_setting.SNOWFLAKE_PASSWORD
SNOWFLAKE_ACCOUNT = env_setting.SNOWFLAKE_ACCOUNT
SNOWFLAKE_WAREHOUSE = env_setting.SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE = env_setting.SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA = env_setting.SNOWFLAKE_SCHEMA
SNOWFLAKE_ROLE=env_setting.SNOWFLAKE_ROLE
SNOWFLAKE_STAGE_NAME = "product_ingestion_stage"  # Snowflake internal stage name
SNOWFLAKE_TABLE_NAME = "products"  # Snowflake target table
BATCH_SIZE = 100  # Number of messages to accumulate before loading
BATCH_TIME_LIMIT_SECONDS = 30  # Max time to wait (in seconds) before loading a batch
