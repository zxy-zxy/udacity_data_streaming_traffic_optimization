"""Methods pertaining to weather data"""
from enum import IntEnum
from pathlib import Path
import json
import logging
import random

import requests

from models.producer import Producer


logger = logging.getLogger(__name__)

REST_PROXY_URL = "http://localhost:8082"


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum("status", "sunny partly_cloudy cloudy windy precipitation", start=0)

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        topic_name = "org.chicago.cta.weather.v1"

        super().__init__(
            topic_name=topic_name,
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        headers = {"Content-Type": "application/vnd.kafka.avro.v2+json"}
        records = (
            [
                {
                    "value": {"temperature": self.temp, "status": self.status.name},
                    "key": {"timestamp": self.time_millis()},
                }
            ]
        )

        data = {
            "key_schema": json.dumps(Weather.key_schema),
            "value_schema": json.dumps(Weather.value_schema),
            "records": records,
        }

        url = f"{REST_PROXY_URL}/topics/{self.topic_name}"
        serialized_data = json.dumps(data)

        try:
            resp = requests.post(
                url,
                headers=headers,
                data=serialized_data,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Error occurred while sending data to REST Proxy {REST_PROXY_URL}.")
            logging.error(f"Error text: {e}.")

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )