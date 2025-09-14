"""
json_consumer_case.py

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

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting author occurrences

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
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
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up Data Store to hold pet counts
#####################################

# Initialize a dictionary to store pet counts
# The defaultdict type initializes counts to 0
# pass in the int function as the default_factory
# to ensure counts are integers
# {pet: count} author is the key and count is the value
pet_counts: defaultdict[str, int] = defaultdict(int)


#####################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        from typing import Any
        message_dict: dict[str, Any] = json.loads(message)

        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        # Extract the 'pet' field from the Python dictionary
        pet = message_dict.get("pet", "unknown")
        logger.info(f"Message received from pet: {pet}")

        # Increment the count for the pet
        pet_counts[pet] += 1

        # Log the updated counts
        logger.info(f"Updated pet counts: {dict(pet_counts)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Performs analytics on messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        while True:
            # poll returns a dict: {TopicPartition: [ConsumerRecord, ...], ...}
            records = consumer.poll(timeout_ms=1000, max_records=100)
            if not records:
                continue

            for _tp, batch in records.items():
                for msg in batch:
                    # value_deserializer in utils_consumer already decoded this to str
                    message_str: str = msg.value
                    logger.debug(f"Received message at offset {msg.offset}: {message_str}")
                    process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        
    logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
