FROM python:3

WORKDIR /usr/src/app

COPY kafkaconsumer.py ./

RUN pip3 install kafka-python

CMD ["python3", "./kafkaconsumer.py"]
