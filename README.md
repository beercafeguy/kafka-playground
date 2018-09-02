# kafka-playground
Code snips for Kafka

#### Note
* Old producer and consumer API (before kafka 0.8) was using ZK to store offset. The new API uses Kafka to store Offset.

#### Common commands 
##### List Topics
`kafka-topics.sh --list --zookeeper kafka1.host.beercafeguy.com:2181,kafka2.host.beercafeguy.com:2181,kafka3.host.beercafeguy.com:2181`
<br>

##### Create Topic
`kafka-topics.sh --create --topic user_clicks --partitions 3 --replication-factor 1 --zookeeper kafka1.host.beercafeguy.com:2181,kafka2.host.beercafeguy.com:2181,kafka3.host.beercafeguy.com:2181`
<br>

##### Delete Topic (This will only work if delete.topic.enable is set to true)
`kafka-topics.sh --delete --topic user_clicks --zookeeper kafka1.host.beercafeguy.com:2181,kafka2.host.beercafeguy.com:2181,kafka3.host.beercafeguy.com:2181`

##### Produce messages from console
`kafka-console-producer.sh --topic seller_survey --broker-list kafka1.host.beercafeguy.com:9092,kafka2.host.beercafeguy.com:9092,kafka3.host.beercafeguy.com:9092`
<br>
##### Consume message using console consumer
`kafka-console-consumer.sh --topic seller_survey --from-beginning --bootstrap-server kafka1.host.beercafeguy.com:9092,kafka2.host.beercafeguy.com:9092,kafka3.host.beercafeguy.com:9092`
<br>
