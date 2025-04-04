from confluent_kafka import Consumer, KafkaException
import json
import psycopg2
import logging
import os
from dotenv import load_dotenv
from datetime import datetime

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('weather_consumer')

# Tải biến môi trường
load_dotenv()

def process_weather_data():
    # Cấu hình Kafka consumer
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
        
        logger.info("Kết nối thành công đến PostgreSQL")
        
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    logger.info(f"Đã đọc hết dữ liệu trong phân vùng {msg.partition()}")
                else:
                    logger.error(f"Lỗi: {msg.error()}")
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
                logger.info(f"Đã lưu dữ liệu thời tiết cho {city} lúc {timestamp}")
                
            except json.JSONDecodeError as e:
                logger.error(f"Lỗi giải mã JSON: {e}")
            except psycopg2.Error as e:
                logger.error(f"Lỗi cơ sở dữ liệu: {e}")
                conn.rollback()
            except Exception as e:
                logger.error(f"Lỗi không mong đợi: {e}")
        
    except KeyboardInterrupt:
        logger.info("Dừng consumer")
    finally:
        consumer.close()
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()
            logger.info("Đã đóng kết nối PostgreSQL")

if __name__ == "__main__":
    process_weather_data()
