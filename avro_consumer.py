from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from schema_client_registry import SchemaClient
from confluent_kafka.serialization import SerializationContext, MessageField


class AvroConsumerClass:
    def __init__(self, bootstrap_server, topic, group_id, schema_client_registry, schema_str):
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.group_id = group_id
        self.consumer = Consumer({'bootstrap.servers': self.bootstrap_server, 'group.id': self.group_id,  "auto.offset.reset": "latest"})
        self.schema_client_registry = schema_client_registry
        self.schema_str = schema_str
        self.value_deserializer = AvroDeserializer(schema_client_registry, schema_str)

    def consume_messages(self):
        self.consumer.subscribe([self.topic])
        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    print(f"Error while consuming message: {msg.error()}")
                    continue
                message = msg.value()
                deserialized_message = self.value_deserializer(message, SerializationContext(self.topic, MessageField.VALUE))
                print(f"Deserialized Message Consumed: {deserialized_message}, Type is: {type(deserialized_message)}")
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == "__main__":
    bootstrap_server = "localhost:19092"
    topic = "test-topic"
    group_id = "my-group-id"
    schema_url = "http://localhost:18081"
    schema_type = "AVRO"

    with open("schema.avsc") as avro_file:
        avro_schema = avro_file.read()

    schema_client = SchemaClient(schema_url, topic, avro_schema, schema_type)

    schema_str = schema_client.get_schema_str()
    consumer = AvroConsumerClass(bootstrap_server, topic, group_id, schema_client.schema_registry_client, schema_str)
    consumer.consume_messages()
