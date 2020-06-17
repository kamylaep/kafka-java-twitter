# Kafka Twitter Producer

Get tweets from Twitter and send them to Kafka.

## Configuration

The following system properties are mandatory:
- For Twitter client (You can obtain their values on the [Twitter Developer site](https://developer.twitter.com/en.html)):
    - API_KEY
    - API_SECRET_KEY
    - ACCESS_TOKEN
    - ACCESS_TOKEN_SECRET
- For the app:
    - KAFKA_BOOTSTRAP_SERVER
    - KAFKA_OUTPUT_TOPIC
    - TWITTER_TERMS

Create the output topic:

```shell script
$ docker exec kafka kafka-topics --zookeeper zookeeper:2181 --topic twitter-in --create --partitions 5 --replication-factor 1
```

Consume from the topic:

```shell script
$ docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic twitter-in --from-beginning --property print.key=true --property print.value=true 
```

You can use this [docker-compose.yaml](https://github.com/kamylaep/docker/blob/master/kafka/docker-compose.yml) to create a single node Kafka cluster.