from confluent_kafka import Consumer


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
                print(f"Message Consumed: {msg.value().decode('utf-8')}")
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
