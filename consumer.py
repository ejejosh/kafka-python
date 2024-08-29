from confluent_kafka import Consumer
import json


class ConsumerClass:
    def __init__(self, bootstrap_server, topic, group_id):
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.group_id = group_id
        self.consumer = Consumer({'bootstrap.servers': self.bootstrap_server, 'group.id': self.group_id})

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
                print(f"Message Consumed: {msg.value()}, Type is: {type(msg.value())}")
                print(f"Deserialized Message Consumed: {msg.value().decode('utf-8')}, Type is: {type(msg.value().decode('utf-8'))}")
                dict_message = json.loads(msg.value().decode('utf-8'))
                print(dict_message, type(dict_message))
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == "__main__":
    bootstrap_server = "localhost:19092"
    topic = "test-topic"
    group_id = "my-group-id"

    consumer = ConsumerClass(bootstrap_server, topic, group_id)
    consumer.consume_messages()
