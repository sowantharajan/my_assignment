import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json



kafka_server = "localhost:9092" //"172.22.0.3:9092"
kafka_topic = "oetker_data_topic"


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

sc = SparkContext(appName="oetker-consumer-pyspark")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 60)

kafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], {"metadata.broker.list": kafka_server})

def is_valid_ip_format(ip):
    if not ip:
        return False

    a = ip.split('.')

    if len(a) != 4:
        return False
    for x in a:
        if not x.isdigit():
            return False
        i = int(x)
        if i < 0 or i > 255:
            return False
    return True


def is_valid_date(date_value):
    try:
        datetime.datetime.strptime(date_value, '%d/%m/%Y')
    except ValueError:
        return False

    return True


def is_valid(row):
    valid_ip_format = is_valid_ip_format(row.get('ip_address'))
    valid_date = is_valid_date(row.get('date'))

    return valid_ip_format and valid_date


def clean_entry():
    def meth(row):
        # Uppercase first letter of the country
        # e.g. Germany, germany, GeRmany -> Germany
        country = row['country'].lower().capitalize()
        row['country'] = country
        return Row(**row)
    return meth


# Load the streamed data - messages
# Filter out improper data - valid items
stream_messages = kafkaStream.map(lambda x: json.loads(x[1]))
valid_items = stream_messages.filter(lambda item: is_valid(item))
messages = valid_items.map(clean_entry())


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
