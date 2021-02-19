"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "org.chicago.cta.stations"


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    logger.info(f"Attempt to create connector: {CONNECTOR_NAME}")

    # every 12 hours
    poll_interval_ms = 60 * 60 * 12 * 1000

    # rows limit
    batch_max_rows = 500

    serialized_data = json.dumps(
        {
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": f"{batch_max_rows}",
                "connection.url": "jdbc:postgresql://postgres:5432/cta",
                "connection.user": "cta_admin",
                "connection.password": "chicago",
                "table.whitelist": "stations",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": "org.chicago.cta.",
                "poll.interval.ms": f"{poll_interval_ms}",
            },
        }
    )

    try:
        resp = requests.post(
            KAFKA_CONNECT_URL,
            headers={"Content-Type": "application/json"},
            data=serialized_data,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"An error occurred during creation of connector: {e}")


if __name__ == "__main__":
    configure_connector()
