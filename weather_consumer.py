from confluent_kafka import Consumer, KafkaError
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

class WeatherConsumer:
    def __init__(self):
        # Cấu hình Kafka consumer
        self.consumer_config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
            'group.id': 'weather_consumer_group',
            'auto.offset.reset': 'earliest'
        }
        
        # Cấu hình PostgreSQL
        self.db_config = {
            'host': os.getenv('DB_HOST', 'postgres'),
            'database': os.getenv('DB_NAME', 'weather_data'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres')
        }
        
        self.topic = os.getenv('KAFKA_TOPIC', 'weather-data-topic')
        
    def connect_db(self):
        """Kết nối đến PostgreSQL database"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            logger.info("Kết nối thành công đến PostgreSQL")
            return conn, cursor
        except psycopg2.Error as e:
            logger.error(f"Lỗi kết nối PostgreSQL: {e}")
            raise

    def parse_weather_data(self, data):
        """Trích xuất dữ liệu thời tiết từ JSON"""
        city = data['name']
        current = data['current']
        
        timestamp = datetime.strptime(current['dt'], '%Y-%m-%d %H:%M:%S')
        temperature = current['temp']
        humidity = current['humidity']
        
        fields = {
            'feels_like': current.get('feels_like'),
            'pressure': current.get('pressure'),
            'wind_speed': current.get('wind_speed'),
            'uv': current.get('uvi'),
            'clouds': current.get('clouds'),
            'visibility': current.get('visibility'),
            'precipitation': current.get('rain', {}).get('1h', 0) if 'rain' in current else 0
        }
        
        weather_info = current.get('weather', [{}])[0] if current.get('weather') else {}
        fields.update({
            'weather_condition': weather_info.get('main'),
            'weather_description': weather_info.get('description'),
            'weather_icon': weather_info.get('icon')
        })
        
        fields.update({
            'sunrise': datetime.strptime(current['sunrise'], '%Y-%m-%d %H:%M:%S') if 'sunrise' in current else None,
            'sunset': datetime.strptime(current['sunset'], '%Y-%m-%d %H:%M:%S') if 'sunset' in current else None
        })
        
        return city, timestamp, temperature, humidity, fields

    def save_to_database(self, cursor, city, timestamp, temperature, humidity, fields):
        """Lưu dữ liệu vào database"""
        query = """
        INSERT INTO weather (
            city, timestamp, temperature, feels_like, humidity, pressure,
            wind_speed, uv, clouds, weather_condition, weather_description,
            sunrise, sunset, visibility, precipitation, weather_icon
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city, timestamp) DO NOTHING
        """
        
        values = (
            city, timestamp, temperature, fields['feels_like'], humidity, fields['pressure'],
            fields['wind_speed'], fields['uv'], fields['clouds'], 
            fields['weather_condition'], fields['weather_description'],
            fields['sunrise'], fields['sunset'], fields['visibility'], 
            fields['precipitation'], fields['weather_icon']
        )
        
        cursor.execute(query, values)
        return timestamp

    def process_messages(self):
        """Xử lý tin nhắn từ Kafka và lưu vào PostgreSQL"""
        consumer = Consumer(self.consumer_config)
        consumer.subscribe([self.topic])
        
        conn = None
        cursor = None
        
        try:
            conn, cursor = self.connect_db()
            
            while True:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Lỗi Kafka: {msg.error()}")
                        continue
                
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    city, timestamp, temperature, humidity, fields = self.parse_weather_data(data)
                    
                    # Lưu vào database
                    saved_timestamp = self.save_to_database(cursor, city, timestamp, temperature, humidity, fields)
                    conn.commit()
                    
                    logger.info(f"Đã lưu dữ liệu thời tiết cho {city} lúc {saved_timestamp}")
                    
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
            if consumer:
                consumer.close()
            if conn:
                if cursor:
                    cursor.close()
                conn.close()
                logger.info("Đã đóng kết nối PostgreSQL")

def main():
    consumer = WeatherConsumer()
    consumer.process_messages()

if __name__ == "__main__":
    main()