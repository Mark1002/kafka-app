"""Producer for kafka."""
import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition())) # noqa


def time_millis():
    """Use this function to get the key for Kafka Events"""
    return str(round(time.time() * 1000))


def check_topic_exists(client: AdminClient, topic: str) -> bool:
    """Checks if the given topic exists in Kafka."""
    topic_metadata = client.list_topics(timeout=5)
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))


def create_topics(client: AdminClient, topics: list):
    """Create kafka topic."""
    new_topics = [
        NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics # noqa
    ]
    result = client.create_topics(new_topics)
    for topic, f in result.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")


def main():
    """Main."""
    datas = ['apple1', 'banana2', 'ggc3']
    conf = {
        'bootstrap.servers': '127.0.0.1:9092',
    }
    admin_clinet = AdminClient({'bootstrap.servers': '127.0.0.1:9092'})
    topic_name = 'local.test.topic'
    if not check_topic_exists(admin_clinet, topic_name):
        create_topics(admin_clinet, [topic_name])
    else:
        print(f'topic {topic_name} alreadly exist!')

    p = Producer(conf)
    for data in datas:
        p.poll(0)
        p.produce(
            topic_name,
            key=time_millis(),
            value=data.encode('utf-8'),
            on_delivery=delivery_report
        )
    p.flush()


if __name__ == '__main__':
    main()
