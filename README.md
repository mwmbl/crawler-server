Mwmbl Crawler Server
====================

The Mwmbl crawler is a distributed crawler with a central server. The central server collects batches of crawled data
from the clients and stores it in long term storage for indexing and analysis.

## Docker setup
Clone this repository ```git clone https://github.com/mwmbl/crawler-server``` then enter ```cd crawler-server```
To start crawler server via docker first edit .env file and then run ```docker-compose up -d```
To rebuild image execute ```docker-compose build```
