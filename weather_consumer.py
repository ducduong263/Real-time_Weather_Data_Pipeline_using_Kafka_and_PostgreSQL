from confluent_kafka import Consumer, KafkaException
import json
import psycopg2
import logging
import os
from dotenv import load_dotenv
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('weather_consumer')

# Load environment variables
load_dotenv()

def process_weather_data():
    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'group.id': 'weather_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_config)
    
    try:
        consumer.subscribe(['weather-data-topic'])
        
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'weather_data'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres')
        }
        
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        logger.info("Connected to PostgreSQL")
        
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    logger.info(f"Reached end of partition {msg.partition()}")
                else:
                    logger.error(f"Error: {msg.error()}")
                continue
            
            try:
                message_value = msg.value().decode('utf-8')
                data = json.loads(message_value)
                city = data['name']
                current = data['current']
                timestamp = datetime.strptime(current['dt'], '%Y-%m-%d %H:%M:%S')
                
                temperature = current['temp']
                feels_like = current.get('feels_like')
                humidity = current['humidity']
                pressure = current.get('pressure')
                wind_speed = current.get('wind_speed')
                wind_deg = current.get('wind_deg')
                clouds = current.get('clouds')
                
                weather_condition = current['weather'][0]['main'] if 'weather' in current and len(current['weather']) > 0 else None
                weather_description = current['weather'][0]['description'] if 'weather' in current and len(current['weather']) > 0 else None
                
                sunrise = datetime.strptime(current['sunrise'], '%Y-%m-%d %H:%M:%S') if 'sunrise' in current else None
                sunset = datetime.strptime(current['sunset'], '%Y-%m-%d %H:%M:%S') if 'sunset' in current else None
                
                query = """
                INSERT INTO weather (
                    city, timestamp, temperature, feels_like, humidity, pressure,
                    wind_speed, wind_deg, clouds, weather_condition, weather_description,
                    sunrise, sunset
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (city, timestamp) DO NOTHING
                """
                
                values = (
                    city, timestamp, temperature, feels_like, humidity, pressure,
                    wind_speed, wind_deg, clouds, weather_condition, weather_description,
                    sunrise, sunset
                )
                
                cursor.execute(query, values)
                conn.commit()
                logger.info(f"Inserted weather data for {city} at {timestamp}")
                
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON: {e}")
            except psycopg2.Error as e:
                logger.error(f"Database error: {e}")
                conn.rollback()
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
        
    except KeyboardInterrupt:
        logger.info("Consumer shutting down")
    finally:
        consumer.close()
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()
            logger.info("PostgreSQL connection closed")

if __name__ == "__main__":
    process_weather_data()
