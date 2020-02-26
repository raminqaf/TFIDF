kafka-topics --create --topic streams-plaintext-input \
  --zookeeper localhost:2181 --partitions 1 --replication-factor 1

kafka-topics --create --topic streams-output \
  --zookeeper localhost:2181 --partitions 1 --replication-factor 1
