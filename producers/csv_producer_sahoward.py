"""
csv_producer_sahoward.py

Stream numeric data to a Kafka topic.

It is common to transfer csv data as JSON so 
each field is clearly labeled. 
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time  # control message intervals
from pathlib import Path  # work with file paths
import csv  # handle CSV data
import json  # work with JSON data
from datetime import datetime  # work with timestamps
import random

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("POOL_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("POOL_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Set up Paths
#####################################

# The parent directory of this file is its folder.
# Go up one more parent level to get the project root.
PROJECT_ROOT = Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

# Set directory where data is stored
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FOLDER.mkdir(parents=True, exist_ok=True)   # ✅ ensure folder exists
logger.info(f"Data folder: {DATA_FOLDER}")

# Set the name of the data file
DATA_FILE = DATA_FOLDER / "pool_temps.csv"
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Message Generator
#####################################

def generate_temperature_csv(file_path: Path, num_records: int = 100):
    """
    Generate a CSV file with random pool temperatures between 60 and 99,
    each with a timestamp.

    Args:
        file_path (Path): Path to the CSV file.
        num_records (int): Number of rows to generate.
    """
    with open(file_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["timestamp", "temperature"])
        writer.writeheader()
        for _ in range(num_records):
            writer.writerow({
                "timestamp": datetime.utcnow().isoformat(),
                "temperature": random.randint(60, 99)
            })

def generate_messages(file_path: Path):
    """
    Read from a CSV file and yield records one by one, continuously.

    Args:
        file_path (Path): Path to the CSV file.

    Yields:
        dict: A message containing timestamp and temperature.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {file_path}")
            with open(file_path, "r", newline="") as csv_file:
                logger.info(f"Reading data from file: {file_path}")

                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    if "temperature" not in row or "timestamp" not in row:
                        logger.error(f"Missing required column in row: {row}")
                        continue

                    message = {
                        "timestamp": row["timestamp"],
                        "temperature": int(row["temperature"]),
                    }
                    logger.debug(f"Generated message: {message}")
                    yield message
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            break

#####################################
# Define main function for this module.
#####################################


def main():
    """
    Main entry point for the producer.

    - Reads the Kafka topic name from an environment variable.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams messages to the Kafka topic.
    """

    logger.info("START producer.")
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Verify the data file exists
    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for csv_message in generate_messages(DATA_FILE):
            producer.send(topic, value=csv_message)
            logger.info(f"Sent message to topic '{topic}': {csv_message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END producer.")


#####################################
# Conditional Execution
#####################################
if not DATA_FILE.exists():
    logger.warning(f"Data file not found: {DATA_FILE}. Creating new CSV...")
    generate_temperature_csv(DATA_FILE, num_records=200)
    
if __name__ == "__main__":
    main()
