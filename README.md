# my_assignment
Kafka Producer and Consumer using Python language

Already created the Docker images based on this source and pushed into Docker Hub. Latest images available in Dockey hub.

Directly execute the docker-compose.yml file
1. Inside consumer directory docker-compose.yml for both producer and consumer (path: consumer/docker-compose.yml)
2. Outside default docker-compose.yml for only producer job(Reading Json file from that input shared path and produce to Kafka topic)

This is very short and sweet project code without more packages and others. Used Direct Stream , Lambda function and RDD without any DF and Datastream


How to run - local mode

Consumer streaming job

Download Spark. Install py dependencies. pip3 install -t dependencies -r <path_to>/dockers/consumer/requirements.txt --no-cache-dir cd dependencies && zip -r ../dependencies.zip .

This will essentially create a folder with required packages downloaded. Navigate to the folder and zip into one.

export PYSPARK_PYTHON=python3 to use Python 3

./bin/spark-submit \
      --jars <path_to>/dockers/consumer/spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar \
      --py-files <path_to>/dependencies.zip \
      <path_to>/consumerPySpark/consumerPySpark.py
