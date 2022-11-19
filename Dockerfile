FROM python:3.8.15

COPY . /pyraft

RUN pip install kazoo

ENV PYTHONPATH=/pyraft
