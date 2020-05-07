import os
import urllib.request
import json
from kafka import KafkaProducer
import time


#env-variables:
kafka_server = os.environ["KAFKA_SERVER"]
kafka_topic = os.environ["KAFKA_TOPIC"]
data_link = os.environ["DATA_ORIGIN"]

producer = KafkaProducer(bootstrap_servers=kafka_server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def produce_jsondata(data_link, kafka_topic):
	data = read_jsondata(data_link)
	for message in data:
		publish(message, kafka_topic)


def read_jsondata(data_link):
	data = urllib.request.urlopen(data_link).read().decode('utf-8')
	data = json.loads(data)
	return data


def publish(message, kafka_topic):
	producer.send(kafka_topic, message)
	

while(True):
	produce_jsondata(data_link, kafka_topic)
	time.sleep(60)
