"""
json_consumer_sahoward.py

Consume json messages from a Kafka topic and process them.

JSON is a set of key:value pairs. 

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\", \"pet\": \"dog\", \"color\": \"white\"}"

Example JSON message (after deserialization) to be analyzed
{"message": "I love Python!", "author": "Eve", "pet": "dog", "color": "white"}

"""

#####################################
# Import Modules
#####################################

import os
import json
import time
from collections import defaultdict
from kafka import KafkaConsumer, errors
from dotenv import load_dotenv
from utils.utils_logger import logger

load_dotenv()

# -----------------------------
# Configuration
# -----------------------------
TOPIC = os.getenv("BUZZ_TOPIC", "buzzline_json")
GROUP_ID = os.getenv("BUZZ_CONSUMER_GROUP_ID", "buzz_group")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER", "localhost:9092")
RETRIES = 5
RETRY_DELAY = 5  # seconds

# Data stores
pet_counts = defaultdict(int)
match_count = defaultdict(int)

# -----------------------------
# Helper Functions
# -----------------------------
def create_consumer_with_retry(topic, group_id, retries=RETRIES, delay=RETRY_DELAY):
    """Create KafkaConsumer with retries if broker is unreachable."""
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: m.decode("utf-8"),
                consumer_timeout_ms=2000,  # prevents indefinite blocking
            )
            logger.info("Kafka consumer created successfully.")
            return consumer
        except errors.NoBrokersAvailable:
            logger.warning(f"[Attempt {attempt}/{retries}] Broker not available. Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError(f"Failed to connect to Kafka broker at {BOOTSTRAP_SERVERS}")

def process_message(message: str):
    """Process a single Kafka message."""
    try:
        message_dict = json.loads(message)
        pet = message_dict.get("pet", "unknown")
        pet_counts[pet] += 1
        logger.info(f"Received message from pet: {pet}")
        logger.info(f"Updated pet counts: {dict(pet_counts)}")

        # Alert logic
        if pet.strip().lower() == "goat":
            match_count["goat"] += 1
            logger.warning(f"🚨 ALERT: Goat detected! Total matches: {match_count['goat']}")
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

# -----------------------------
# Main Function
# -----------------------------
def main():
    logger.info("START consumer.")
    logger.info(f"Kafka topic: {TOPIC}, group: {GROUP_ID}, broker: {BOOTSTRAP_SERVERS}")

    try:
        consumer = create_consumer_with_retry(TOPIC, GROUP_ID)
    except RuntimeError as e:
        logger.error(f"Could not start consumer: {e}")
        return

    logger.info(f"Polling messages from topic '{TOPIC}'...")

    try:
        for msg in consumer:
            process_message(msg.value)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except errors.KafkaError as e:
        logger.error(f"Kafka error during consumption: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{TOPIC}' closed.")
        logger.info("END consumer.")

# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    main()