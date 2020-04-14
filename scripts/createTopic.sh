#!/bin/bash
cd /usr/local/kafka/kafka_2.11-2.2.0
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test
cd ~
