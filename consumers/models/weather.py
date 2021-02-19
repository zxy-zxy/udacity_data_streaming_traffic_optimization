"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("Handling incoming weather data.")
        try:
            message_value = message.value()
            status = message_value["status"]
            temperature = message_value["temperature"]
            logger.debug(f"Incoming message with status: {status}, temperature: {temperature}")
            self.status = status
            self.temperature = temperature
        except KeyError as e:
            logger.error(e)
