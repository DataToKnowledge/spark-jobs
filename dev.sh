#!/usr/bin/env bash

if [ "$#" -eq 0 ]; then
    cat << 'EOF'
Usage: ./dev.sh start | ps | stop | restart | rm | logs

Example:
$: ./dev.sh start -- start elasticsearch, zookeeper and kafka

EOF
    exit 1
fi

compose_path=dockerfile/dev-images/elasticsearch

case $1 in
    start)
        cd $compose_path && docker-compose up -d
        ;;

    ps)
        cd $compose_path && docker-compose ps
        ;;

    rm)
        cd $compose_path && docker-compose stop && docker-compose rm
        ;;

    stop)
        cd $compose_path && docker-compose stop
        ;;

    restart)
        cd $compose_path && docker-compose restart
        ;;

    logs)
        cd $compose_path && docker-compose logs
        ;;
    *)
        echo "the command ${1} is not recognized"
        exit 1
        ;;
esac