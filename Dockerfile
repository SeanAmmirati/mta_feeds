# Python 3.6.7
FROM python:3.6.7-alpine3.6
# author of file
# LABEL maintainer=”Sean Ammirati <sammirati@statsworks.info>”

COPY . /mta_feeds/
WORKDIR /mta_feeds 

RUN apk add --no-cache --virtual build-base libffi
RUN pip install -r requirements.txt

