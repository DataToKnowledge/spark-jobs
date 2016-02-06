# General

this repository includes the base logis used to extract feeds from the web and the spark jobs to download the feeds.


## Development (READ IT BEFORE STARTING!!!! )

for development it need also a submodule [dockerfile](https://github.com/DataToKnowledge/dockerfile) that point to the
docker images used for production ad development.

**Clone the repository using**

```bash

git clone --recursive git@github.com:DataToKnowledge/sparkler.git

```

Sparkler needs Kafka and Elasticsearch that can be ran using the script `dev.sh`

```bash
Usage: ./dev.sh start | ps | stop | restart | rm | logs

Example:
$: ./dev.sh start -- start elasticsearch, zookeeper and kafka
```

This commands are a wrapper to docker-compose commands.