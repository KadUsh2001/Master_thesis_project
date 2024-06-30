from Master_thesis_project.utils import data_preprocessing, kafka_processing, send_api_data,process_message, data_show
from datetime import datetime
import requests
import pandas as pd
import numpy as np
import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves
from kafka import KafkaProducer,KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time
import warnings
warnings.filterwarnings("ignore")

time_now = datetime.now()
admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')
topic_name="test-10"
topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
admin_client.create_topics([topic])

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserialize from JSON
)
i=0

while True:
        time_now = datetime.now()
        url = "https://prim.iledefrance-mobilites.fr/marketplace/estimated-timetable?LineRef=STIF%3ALine%3A%3AC01377%3A"
        headers = {'Accept': 'application/json','apikey': "OUe5wQhYRvtixxGZknneSVS1N4pR2O1x"}
        req = requests.get(url, headers=headers)
        print('Status:',req)
        test_pipeline = data_preprocessing(req)
        send_api_data(producer, topic_name, test_pipeline)

        time.sleep(10)

        message = next(consumer)
        df_after_kafka = process_message(message.value)
        data_C, data_I, data_V = kafka_processing(df_after_kafka, time_now)
        data_C.to_csv(f"kafka_data/data_C.csv", index=False)
        data_I.to_csv(f"kafka_data/data_I.csv", index=False)
        data_V.to_csv(f"kafka_data/data_V.csv", index=False)
        print("Saved dataframes")

        time.sleep(60)
        data_C_historical = pd.read_csv("kafka_data/data_C_historical.csv")
        data_I_historical = pd.read_csv("kafka_data/data_I_historical.csv")
        data_V_historical = pd.read_csv("kafka_data/data_V_historical.csv")
        data_C_show= data_show(data_C)
        data_I_show= data_show(data_I)
        data_V_show= data_show(data_V)
        data_C_historical = pd.concat((data_C_historical,data_C_show))
        data_I_historical = pd.concat((data_I_historical,data_I_show))
        data_V_historical = pd.concat((data_V_historical,data_V_show))
        data_C_historical.to_csv("kafka_data/data_C_historical.csv", index=False)
        data_I_historical.to_csv("kafka_data/data_I_historical.csv", index=False)
        data_V_historical.to_csv("kafka_data/data_V_historical.csv", index=False)
        i += 1


