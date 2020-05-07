FROM python:3

WORKDIR /usr/src/app

COPY producer.py ./

RUN pip3 install kafka-python

CMD ["python3", "./kafkaproducer.py"]
