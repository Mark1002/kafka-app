"""Producer for kafka."""
import time
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


def main():
    """Main."""
    datas = ['apple1', 'banana2', 'ggc3']
    conf = {
        'bootstrap.servers': '192.168.1.131:9092,192.168.1.132:9093,192.168.1.133:9094', # noqa
    }
    p = Producer(conf)

    for data in datas:
        p.poll(0)
        p.produce(
            'my-first-topic',
            key=time_millis(),
            value=data.encode('utf-8'),
            on_delivery=delivery_report
        )
    p.flush()


if __name__ == '__main__':
    main()
