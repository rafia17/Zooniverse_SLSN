from confluent_kafka import Consumer, KafkaError, Message

class msgConsumer():
    def __init__(self, kafka_server, group_id):
        self.group_id = group_id

        conf = {
            'bootstrap.servers': kafka_server,
            'group.id': self.group_id,
            'default.topic.config': {'auto.offset.reset': 'smallest'}
        }
        self.streamReader = Consumer(conf)

    def topics(self):
        t = self.streamReader.list_topics()
        t = t.topics
        t = t.keys()
        t = list(t)
        return t

    def subscribe(self, topic):
        self.streamReader.subscribe([topic])

    def poll(self):
        try:
            msg = self.streamReader.poll(timeout=30)
            return msg.value()
        except:
            return None

    def close(self):
        self.streamReader.close()

