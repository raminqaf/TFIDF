kafka-console-consumer --topic streams-output \
  --from-beginning \
  --bootstrap-server localhost:9092 \
  --property print.key=true \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
