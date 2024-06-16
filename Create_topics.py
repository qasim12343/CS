import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import tkinter as tk
from tkinter import filedialog
import json


def select_file():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    return file_path


def create_kafka_topics(categories, bootstrap_servers='localhost:9092'):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    topics = [NewTopic(f"{category}_topic", num_partitions=1,
                       replication_factor=1) for category in categories]

    fs = admin_client.create_topics(topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")


def send_to_kafka(df, category_field, bootstrap_servers='localhost:9092'):
    unique_categories = df[category_field].unique()
    create_kafka_topics(unique_categories, bootstrap_servers)

    producer = Producer({'bootstrap.servers': bootstrap_servers})

    for category in unique_categories:
        topic = f"{category}_topic"
        category_df = df[df[category_field] == category]

        for record in category_df.to_dict(orient='records'):
            # Convert the record to a JSON string
            json_record = json.dumps(record)
            producer.produce(topic, key=str(
                record[category_field]), value=json_record)
            producer.poll(0)  # Poll to process delivery reports

    producer.flush()


if __name__ == "__main__":
    file_path = select_file()
    category_field = input("Enter the category field name: ")
    df = pd.read_csv(file_path)
    send_to_kafka(df, category_field)
