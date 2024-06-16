import json
from confluent_kafka import Consumer, KafkaError, KafkaException


def create_consumer(bootstrap_servers='localhost:9092', group_id='my_consumer_group'):
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })
    return consumer


def get_existing_topics(bootstrap_servers='localhost:9092'):
    consumer = create_consumer(bootstrap_servers)
    topics_metadata = consumer.list_topics(timeout=10).topics
    consumer.close()
    return list(topics_metadata.keys())  # Convert keys to list of strings


def subscribe_to_topics(consumer, topics):
    consumer.subscribe(topics)
    print(f"Subscribed to topics: {topics}")


def consume_messages(consumer, timeout=1.0):
    try:
        while True:
            msg = consumer.poll(timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition: {msg.partition()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                try:
                    record_key = msg.key().decode('utf-8')
                    record_value = msg.value().decode('utf-8')
                    # Handle JSON decoding
                    try:
                        record_value = json.loads(record_value)
                        print(f"Consumed record with key: {
                              record_key} and value: {record_value}")
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON: {e}")
                        print(f"Raw message value: {record_value}")
                except UnicodeDecodeError as e:
                    print(f"UnicodeDecodeError: {e}")
                    print(f"Raw message value: {msg.value()}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    bootstrap_servers = 'localhost:9092'
    topics = get_existing_topics(bootstrap_servers)
    if not topics:
        print("No topics found")
    else:
        consumer = create_consumer(bootstrap_servers)
        subscribe_to_topics(consumer, topics)
        consume_messages(consumer)
