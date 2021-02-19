"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)

BROKER_URL = "kafka://localhost:9092"

station_topic_name = "org.chicago.cta.stations"
transformed_station_out_topic_name = "org.chicago.cta.stations.table.v1"


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker=BROKER_URL, store="memory://")
station_topic = app.topic(station_topic_name, value_type=Station)
transformed_station_out_topic = app.topic(
    transformed_station_out_topic_name, value_type=TransformedStation, partitions=1
)

table = app.Table(
    "stations",
    default=TransformedStation,
    partitions=1,
    changelog_topic=transformed_station_out_topic,
)


@app.agent(station_topic)
async def process_station_topic(stations: faust.Stream):
    async for current_event in stations:

        logger.debug(f"Processing station: {current_event}.")

        if current_event.red:
            color = "red"
        elif current_event.blue:
            color = "blue"
        else:
            color = "green"

        transformed_station = TransformedStation(
            station_id=current_event.station_id,
            station_name=current_event.station_name,
            order=current_event.order,
            line=color,
        )

        table[current_event.station_id] = transformed_station


if __name__ == "__main__":
    app.main()
