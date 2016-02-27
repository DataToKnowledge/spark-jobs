# General

this repository includes the base logic used to extract feeds from the web and the spark jobs to download the feeds.

Execute
```bash
./init_subprojects.sh

```
before developing. It will clone the dependent projects

Sparkler needs Kafka and Elasticsearch that can be run using the script `dev.sh`

```bash
Usage: ./dev.sh start | ps | stop | restart | rm | logs

Example:
$: ./dev.sh start -- start elasticsearch, zookeeper and kafka
```

This commands are a wrapper to docker-compose commands.
