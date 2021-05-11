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


# Kafka in local machine 

#### List topics
`kafka-topics.bat --bootstrap-server localhost:9092 --list`

#### Create a topic 
`kafka-topics.bat --bootstrap-server localhost:9092 --topic truck_locations --create`

#### Create a topic with given replication
`kafka-topics.bat --bootstrap-server localhost:9092 --topic truck_locations_rep --replication-factor 1 --partitions 3 --create`
##### Note: 
* Replication factor can never be more then available number of brokers as one broker can not host more then 1 replica of any given partition 

#### Describe a topic
`kafka-topics.bat --bootstrap-server localhost:9092 --topic truck_locations --describe`


#### Produce message in console producer
`kafka-console-producer.bat --bootstrap-server localhost:9092 --topic truck_locations_rep`

#### Produce message in console producer to a non existing topic
`kafka-console-producer.bat --bootstrap-server localhost:9092 --topic truck_locations_rep_ne`
#### Console Consumer
`kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic truck_locations_rep --from-beginning`

#### Same consumer group consuming from same topic  
`kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic truck_locations_rep --from-beginning --group analytics_grp`
`kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic truck_locations_rep --from-beginning --group analytics_grp`
