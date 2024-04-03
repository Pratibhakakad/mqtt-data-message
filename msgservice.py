import paho.mqtt.client as mqtt
import pymongo
import logging
from datetime import datetime

# MQTT configuration
MQTT_BROKER_HOST = 'mqtt.eclipse.org'
MQTT_BROKER_PORT = 1883
MQTT_TOPIC = 'your/topic'

# MongoDB configuration
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
MONGODB_DATABASE = 'mydatabase'
MONGODB_COLLECTION = 'mqtt_messages'

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Establish MongoDB connection
mongo_client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)
db = mongo_client[MONGODB_DATABASE]
collection = db[MONGODB_COLLECTION]

# Callback for MQTT message received
def on_message(client, userdata, message):
    try:
        # Parse and validate message content
        message_payload = message.payload.decode()
        if not message_payload:
            raise ValueError("Empty message received")
        
        # Construct message document
        message_document = {
            "topic": message.topic,
            "payload": message_payload,
            "timestamp": datetime.utcnow()
        }
        
        # Insert the processed message into MongoDB
        collection.insert_one(message_document)
        
        logger.info(f"Inserted message into MongoDB: {message_document}")
        
    except Exception as e:
        # Handle errors gracefully
        logger.error(f"Error processing message: {e}")

# Setup MQTT client
mqtt_client = mqtt.Client(client_id='')
mqtt_client.on_message = on_message

def on_connect(client, userdata, flags, rc):
    logger.info(f"Connected to MQTT broker with result code {rc}")
    mqtt_client.subscribe(MQTT_TOPIC)

mqtt_client.on_connect = on_connect

mqtt_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)

# Start MQTT client loop
mqtt_client.loop_forever()
