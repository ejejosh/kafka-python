from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError


class SchemaClient:

    def __init__(self, schema_url, subject_name, schema_str, schema_type):
        self.schema_url = schema_url
        self.subject_name = subject_name
        self.schema_str = schema_str
        self.schema_type = schema_type
        self.schema_client = SchemaRegistryClient({"url": self.schema_url})

    def schema_exists_in_registry(self):
        try:
            self.schema_client.get_latest_version(self.subject_name)
            return True
        except SchemaRegistryError:
            return False

    def register_schema(self):
        if not self.schema_exists_in_registry():
            try:
                schema = Schema(self.schema_str, self.schema_type)
                self.schema_client.register_schema(self.subject_name, schema)
            except SchemaRegistryError as e:
                print(e)


if __name__ == "__main__":
    schema_url = "http://localhost:18081"
    topic_name = "test-topic"
    # schema_type = "JSON"
    #
    # with open("schema.json") as json_schema:
    #     schema_str = json_schema.read()

    schema_type = "AVRO"

    with open("schema.avsc") as avro_schema:
        schema_str = avro_schema.read()

    client = SchemaClient(schema_url, topic_name, schema_str, schema_type)
    client.register_schema()


