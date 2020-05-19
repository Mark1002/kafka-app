"""Consumer for kafka."""
from confluent_kafka import Consumer, KafkaException


def main():
    """Main."""
    conf = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'local',
        'session.timeout.ms': 6000,
        'auto.offset.reset': 'earliest'
    }
    topic_name = 'local.test.topic'
    c = Consumer(conf)
    c.subscribe([topic_name])
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                print('Waiting for message or event/error in poll()')
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                print(f'{msg.topic()}[{msg.partition()}] at offset {msg.offset()}, key: {msg.key()}, value: {msg.value()}') # noqa
    except KeyboardInterrupt:
        print('aborted by user.')
    finally:
        c.close()


if __name__ == '__main__':
    main()
