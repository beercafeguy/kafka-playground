bin/windows/zookeeper-server-start.bat config/zookeeper.properties
bin/windows/kafka-server-start.bat config/server.properties

bin/windows/kafka-topics.bat --create --zookeeper localhost:2181 \
    --replication-factor 1 --partitions 3 --topic users-table --config cleanup.policy=compact
bin/windows/kafka-topics.bat --create --zookeeper localhost:2181 \
    --replication-factor 1 --partitions 3 --topic user-purchases
bin/windows/kafka-topics.bat --create --zookeeper localhost:2181 \
    --replication-factor 1 --partitions 3 --topic enriched-purchase-data-inner
bin/windows/kafka-topics.bat --create --zookeeper localhost:2181 \
    --replication-factor 1 --partitions 3 --topic enriched-purchase-data-leftjoin

bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 \
--topic enriched-purchase-data-inner \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 \
--topic enriched-purchase-data-leftjoin \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer