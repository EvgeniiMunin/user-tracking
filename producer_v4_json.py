import json
import pandas as pd
from confluent_kafka import Producer, KafkaException


def delivery_report(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % msg.value().decode('utf-8'), str(err))
    else:
        print("Message produced: %s" % msg.value().decode('utf-8'))


if __name__ == "__main__":
    df = pd.read_csv("data/sampled_preprocessed_train_50k.csv")
    print(df.info(), df.head())

    with open("config/producer_config.json", "r") as f:
        producer_config = json.load(f)

    producer = Producer(producer_config)

    for row in df.to_dict(orient='records')[:500]:
        message_id = str(row["id"])
        json_data = json.dumps(row).encode("utf-8")

        print("row: ", row, "\n")
        print("json_data: ", json_data)

        try:
            producer.produce(
                topic="user-tracking-v2",
                key=message_id,
                value=json_data,
                on_delivery=delivery_report
            )
            producer.flush()

        except KafkaException as e:
            print('Kafka failure ', e)

