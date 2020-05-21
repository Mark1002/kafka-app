"""Consumer for kafka."""
import json
from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING


def on_assign(consumer, partitions):
    """Callback for when topic assignment takes place"""
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING
    consumer.assign(partitions)


def perform_consume(reset_offset_beginning: bool = False):
    """Perform consumer."""
    conf = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'local-consumer',
        'session.timeout.ms': 6000,
        'auto.offset.reset': 'earliest'
    }
    topic_name = 'local.test.topic'
    c = Consumer(conf)
    if reset_offset_beginning:
        c.subscribe([topic_name], on_assign=on_assign)
    else:
        c.subscribe([topic_name])
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                print('Waiting for message or event/error in poll()')
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                print(f'{msg.topic()}[{msg.partition()}] at offset {msg.offset()}, key: {msg.key()}') # noqa
                message = json.loads(msg.value())
                print(message)
    except KeyboardInterrupt:
        print('aborted by user.')
    finally:
        c.close()


def main():
    """Main."""
    perform_consume(True)


if __name__ == '__main__':
    main()
