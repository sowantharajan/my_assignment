FROM python:3

WORKDIR /usr/src/app

COPY kafkaproducer.py ./

RUN pip3 install kafka-python

CMD ["python3", "./kafkaproducer.py"]
