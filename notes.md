* List Kafka topics
    ```bash
    kafka-topics.sh --list --zookeeper localhost:2181
    ```
* Reset offset for all topics 
    ```bash
    kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --reset-offsets --to-earliest --group stations-stream --execute --all-topics
    ```
* consumer for org.chicago.cta.stations.table.v1
    ```bash
    kafka-console-consumer.sh --topic "org.chicago.cta.stations.table.v1" \
    --bootstrap-server localhost:9092 --from-beginning
    ```
