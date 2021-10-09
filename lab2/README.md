### How to run?

Start server and create "avg" topic:

```sh
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
kafka-server-start.sh $KAFKA_HOME/config/server.properties
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic avg
```

Install sbt: https://www.scala-sbt.org/download.html

In /src/generator, run:

```sh
sbt run
```

In /src/sparkstreaming, edit built.sbt, change the package version to yours, then run:

```sh
sbt run
```

### Other

#### delete a kafka topic

Add to server.properties file under config folder:

```sh
delete.topic.enable=true
```

run command:

```sh
bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic test
```

#### check list of topics

```sh
kafka-topics.sh --list --zookeeper localhost:2181
```

#### Consume the messages sent to the topic "avg"

```sh
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic avg --from-beginning
```

