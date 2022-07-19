# Kafka Streams Scaffold

Execute "mvn clean package" to build a jar file out of this project!

```shell
./kafka-topics --bootstrap-server localhost:9192 --create --topic streams-plaintext-input --partitions 3 --replication-factor 1
./kafka-topics --bootstrap-server localhost:9192 --create --topic streams-wordcount-output --partitions 3 --replication-factor 1

./kafka-topics --bootstrap-server localhost:9192 --list

./kafka-topics --bootstrap-server localhost:9192 --describe --topic streams-plaintext-input
./kafka-topics --bootstrap-server localhost:9192 --describe --topic streams-plaintext-output
```

```bash
kafka-topics --bootstrap-server pkc-41wq6.eu-west-2.aws.confluent.cloud:9092 --command-config services/src/main/resources/cloud1.properties \
--create --topic demo-json-purchases --partitions 6 --replication-factor 3

kafka-topics --bootstrap-server pkc-41wq6.eu-west-2.aws.confluent.cloud:9092 --command-config services/src/main/resources/cloud1.properties \
--create --topic demo-json-aggregate --partitions 6 --replication-factor 3
```
