from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import pandas as pd
import datetime

data_file= "netflix_dataset.csv"  
netflix_data = pd.read_csv(data_file)

def read_config(filename):
  config = {}
  with open(filename) as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config


config = read_config("client.properties")

schemaConfig = read_config("schema.properties")

schema_registry_conf = {
    "url": schemaConfig["schema.registry.url"],
    "basic.auth.user.info": schemaConfig["schema.registry.basic.auth.user.info"],
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

schema_subject = "netflix_user_behavior-value"

schema = schema_registry_client.get_latest_version(schema_subject).schema.schema_str

avro_serializer = AvroSerializer(schema_registry_client, schema)

config["value.serializer"] = avro_serializer

producer = SerializingProducer(config)

topic = "netflix_user_behavior"

print("Preview of formatted records:")
for _, row in netflix_data.head(20).iterrows():
    timestamp = datetime.datetime.strptime(row["datetime"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
    record_value = {
        "user_id": str(row["user_id"]),
        "movie_title": str(row["title"]),
        "watch_duration": float(row["duration"]),
        "timestamp": str(timestamp)
    }
    print(record_value)

for _, row in netflix_data.iterrows():

    timestamp = datetime.datetime.strptime(row["datetime"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
    record_value = {
        "user_id": str(row["user_id"]),
        "movie_title": str(row["title"]),
        "watch_duration": float(row["duration"]),
        "timestamp": str(timestamp)
    }

    try:
        producer.produce(
            topic=topic,
            key=None, 
            value=record_value
        )
    except BufferError:
        producer.flush()
        producer.produce(
            topic=topic,
            key=None,
            value=record_value
        )

producer.flush()
