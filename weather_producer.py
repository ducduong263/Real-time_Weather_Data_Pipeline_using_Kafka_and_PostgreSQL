from confluent_kafka import Producer
import json
import requests
import time
import logging
import os
from datetime import datetime, timezone
import pytz
from dotenv import load_dotenv

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('weather_producer')

load_dotenv()

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Gửi tin nhắn thất bại: {err}")
    else:
        logger.info(f"Tin nhắn đã được gửi đến {msg.topic()} [{msg.partition()}]")

def format_weather_data(data, city_name):
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    
    data['name'] = city_name
    
    for time_field in ['dt', 'sunrise', 'sunset']:
        if time_field in data['current']:
            dt_utc = datetime.fromtimestamp(data['current'][time_field], tz=timezone.utc)
            dt_vietnam = dt_utc.astimezone(vietnam_tz)
            data['current'][time_field] = dt_vietnam.strftime('%Y-%m-%d %H:%M:%S')
    return data

def fetch_and_send_weather_data():
    producer_config = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'client.id': 'weather_producer'
    }
    producer = Producer(producer_config)
    
    cities = [
        {"name": "Ho Chi Minh", "lon": 106.666672, "lat": 10.75},
        {"name": "Ha Noi", "lon": 105.841171, "lat": 21.0245},
        {"name": "Da Nang", "lon": 108.220833, "lat": 16.06778}
    ]
    
    exclude_parts = "minutely,hourly,daily"
    api_key = os.getenv('OPENWEATHERMAP_API_KEY')
    
    if not api_key:
        logger.error("Không tìm thấy API key của OpenWeatherMap trong biến môi trường")
        return
    
    topic_name = os.getenv('KAFKA_TOPIC', 'weather-data-topic')
    
    for city in cities:
        url = (f"https://api.openweathermap.org/data/3.0/onecall"
               f"?lat={city['lat']}&lon={city['lon']}"
               f"&units=metric&exclude={exclude_parts}&appid={api_key}")
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            weather_data = format_weather_data(response.json(), city["name"])
            
            producer.produce(
                topic_name,
                key=city["name"].encode('utf-8'),
                value=json.dumps(weather_data).encode('utf-8'),
                callback=delivery_report
            )
            
            logger.info(f"Đã lấy và gửi dữ liệu thời tiết cho {city['name']}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy dữ liệu thời tiết cho {city['name']}: {e}")
        except Exception as e:
            logger.error(f"Lỗi không mong muốn: {e}")
    
    producer.flush()

def main():
    interval = int(os.getenv('FETCH_INTERVAL_SECONDS', 600))  # Mặc định 10 phút
    
    try:
        while True:
            fetch_and_send_weather_data()
            logger.info(f"Đang chờ {interval} giây trước khi lấy dữ liệu lần tiếp theo.")
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Dừng chương trình Producer")

if __name__ == "__main__":
    main()