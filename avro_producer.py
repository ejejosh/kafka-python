from producer import ProducerClass
from admin import Admin
from confluent_kafka.schema_registry.avro import AvroSerializer
from schema_client_registry import SchemaClient
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from uuid import uuid4


class User:

    def __init__(self, user_id, first_name, middle_name, last_name, age, email, address):
        self.user_id = user_id
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.age = age
        self.email = email
        self.address = address


def user_to_dict(user):
    return dict(user_id=user.user_id, first_name=user.first_name, middle_name=user.middle_name, last_name=user.last_name, age=user.age, email=user.email, address=user.address)


def delivery_report(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {msg.key()}")
        return
    print(f"Message Successfully Delivered - Key: {msg.key()}, Topic: {msg.topic()}, partition: {msg.partition()}, "
          f"offset: {msg.offset()}")


class AvroProducerClass(ProducerClass):

    def __init__(self, bootstrap_server, topic, schema_registry_client, schema_str, message_size, compression_type, batch_size=None, waiting_time=None):
        super().__init__(bootstrap_server, topic, message_size, compression_type, batch_size, waiting_time)
        self.schema_registry_client = schema_registry_client
        self.schema_str = schema_str
        self.value_serializer = AvroSerializer(schema_registry_client, schema_str)
        self.key_serializer = StringSerializer('utf-8')

    def send_message(self, key=None, value=None):
        try:
            if value:
                print(f"Message size is: {len(value) / (1024 * 1024)}")
                avro_byte_value = self.value_serializer(value, SerializationContext(topic, MessageField.VALUE))
            else:
                avro_byte_value = None
            self.producer.produce(
                                topic=self.topic,
                                key=self.key_serializer(str(key)),
                                value=avro_byte_value,
                                headers={"correlation_id": str(uuid4())},
                                on_delivery=delivery_report)
            # print(f"Message Sent: {avro_byte_message}")
        except Exception as e:
            print(e, len(value) / (1024 * 1024))


if __name__ == "__main__": 
    bootstrap_server = "localhost:19092"
    topic = "test-topic"
    schema_url = "http://localhost:18081"
    schema_type = "AVRO"

    # Create Topic
    a = Admin(bootstrap_server)
    a.create_topic(topic, 2)

    # Register Schema

    with open("schema.avsc") as avro_file:
        avro_schema = avro_file.read()

    schema_client = SchemaClient(schema_url, topic, avro_schema, schema_type)
    schema_client.set_compatibility('FORWARD')
    schema_client.register_schema()

    # Get schema from registry
    schema_str = schema_client.get_schema_str()

    # Produce message
    p = AvroProducerClass(bootstrap_server,
                          topic,
                          schema_client.schema_registry_client,
                          schema_str, message_size=4*1024*1024,
                          compression_type="snappy",
                          batch_size=100_00_00,
                          waiting_time=100_000)

    try:
        while True:
            action = input("Enter 'insert' to add a record or 'delete' for tomnstone: ").strip().lower()
            if action == "insert":
                user_id = int(input("Enter user ID: "))
                first_name = input("Enter your first name: ")
                middle_name = input("Enter your middle name: ")
                last_name = input("Enter your last name: ")
                age = int(input("Enter your age: "))
                email = input("Enter email email address: ")
                address = input("Enter email address: ")
                print("=== Message Sent ===")

                user = User(user_id=user_id, first_name=first_name, middle_name=middle_name, last_name=last_name, age=age, email=email, address=address)

                p.send_message(key=user_id, value=user_to_dict(user))
            elif action == "delete":
                user_id = int(input("Enter user ID: "))
                p.send_message(key=user_id)

    except KeyboardInterrupt:
        pass

    p.commit()
