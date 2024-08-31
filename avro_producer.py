from producer import ProducerClass
from admin import Admin
from confluent_kafka.schema_registry.avro import AvroSerializer
from schema_client_registry import SchemaClient
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from uuid import uuid4


class User:

    def __init__(self, first_name, middle_name, last_name, age):
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.age = age


def user_to_dict(user):
    return dict(first_name=user.first_name, middle_name=user.middle_name, last_name=user.last_name, age=user.age)


def delivery_report(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {msg.key()}")
        return
    print(f"Message Successfully Delivered - Key: {msg.key()}, Topic: {msg.topic()}, partition: {msg.partition()}, "
          f"offset: {msg.offset()}")


class AvroProducerClass(ProducerClass):

    def __init__(self, bootstrap_server, topic, schema_registry_client, schema_str, message_size, compression_type):
        super().__init__(bootstrap_server, topic, message_size, compression_type)
        self.schema_registry_client = schema_registry_client
        self.schema_str = schema_str
        self.value_serializer = AvroSerializer(schema_registry_client, schema_str)
        self.key_serializer = StringSerializer('utf-8')

    def send_message(self, message):
        try:
            # Schema validation
            print(f"Message size is: {len(message) / (1024 * 1024)}")
            avro_byte_message = self.value_serializer(message, SerializationContext(topic, MessageField.VALUE))
            self.producer.produce(
                                topic=self.topic,
                                key=self.key_serializer(str(uuid4())),
                                value=avro_byte_message,
                                headers={"correlation_id": str(uuid4())},
                                on_delivery=delivery_report)
            # print(f"Message Sent: {avro_byte_message}")
        except Exception as e:
            print(e, len(message) / (1024 * 1024))


if __name__ == "__main__":
    bootstrap_server = "localhost:19092"
    topic = "test-topic"
    schema_url = "http://localhost:18081"
    schema_type = "AVRO"

    # Create Topic
    a = Admin(bootstrap_server)
    a.create_topic(topic)

    # Register Schema

    with open("schema.avsc") as avro_file:
        avro_schema = avro_file.read()

    schema_client = SchemaClient(schema_url, topic, avro_schema, schema_type)
    schema_client.register_schema()

    # Get schema from registry
    schema_str = schema_client.get_schema_str()

    # Produce message
    p = AvroProducerClass(bootstrap_server,
                          topic,
                          schema_client.schema_registry_client,
                          schema_str, message_size=4*1024*1024,
                          compression_type="snappy")

    try:
        while True:
            first_name = input("Enter your first name: ") * 100000
            middle_name = input("Enter your middle name: ") * 100000
            last_name = input("Enter your last name: ") * 10000
            age = int(input("Enter your age: "))
            print("=== Message Sent ===")

            user = User(first_name=first_name, middle_name=middle_name, last_name=last_name, age=age)

            p.send_message(user_to_dict(user))
            break
    except KeyboardInterrupt:
        pass

    p.commit()
