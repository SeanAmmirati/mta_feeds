# Python 3.6.7
FROM python:3.6.7-alpine3.6
# author of file
LABEL maintainer=”Sean Ammirati <sammirati@statsworks.info>”

COPY /home/sammirati/git_repositories/mta_feeds /mta_feeds/
WORKDIR /mta_feeds 

RUN ls
RUN apk add --no-cache --virtual gcc
RUN pip install .


