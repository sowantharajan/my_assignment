import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json



kafka_server = "localhost:9092" 
kafka_topic = "oetker_data_topic"


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

sc = SparkContext(appName="oetker-consumer-pyspark")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 60)

kafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], {"metadata.broker.list": kafka_server})

messages = kafkaStream.map(lambda x: json.loads(x[1]))


def n_unique_users(messages):
	users_messages = messages.map(lambda message: (message["first_name"], message["last_name"]))
	users_messages.foreachRDD(lambda rdd: print("Number of unique users: " + str(rdd.distinct().count())))
	

def frecuency_countries(messages):
	countries_messages = messages.map(lambda message: message["country"]).countByValue()
	most_represented_countries_dstream = countries_messages.transform((lambda y: y.sortBy(lambda x:(-x[1]))))
	least_represented_countries_dstream = countries_messages.transform((lambda y: y.sortBy(lambda x:(x[1]))))
	
	most_represented_country = most_represented_countries_dstream.transform(lambda rdd: sc.parallelize(rdd.take(1)))
	most_represented_country.pprint()
	least_represented_country = least_represented_countries_dstream.transform(lambda rdd: sc.parallelize(rdd.take(1)))
	least_represented_country.pprint()


n_unique_users(messages)
frecuency_countries(messages)


ssc.start()
ssc.awaitTermination()
