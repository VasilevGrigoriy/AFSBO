FROM apache/airflow:2.8.1
ARG ACCESS_KEY_ID
ARG SECRET_ACCESS_KEY
ARG ENDPOINT_URL

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends vim \
  && apt-get install -y --option=Dpkg::Options::=--force-confdef git python3 python3-pip make g++ curl gcc libpq-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
ENV ACCESS_KEY_ID=$ACCESS_KEY_ID
ENV SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY
ENV ENDPOINT_URL=$ENDPOINT_URL
COPY requirements.txt /tmp

USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt