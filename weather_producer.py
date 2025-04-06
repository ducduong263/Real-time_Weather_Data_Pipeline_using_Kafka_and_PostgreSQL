from confluent_kafka import Producer
import json
import requests
from datetime import datetime, timezone
import pytz
import time
import logging
import os
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
        logger.info(f"Tin nhắn đã được gửi đến {msg.topic()} [{msg.partition()}] tại offset {msg.offset()}")

def fetch_and_send_weather_data():
    producer_config = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'client.id': 'weather_producer'
    }
    producer = Producer(producer_config)
    
    coordinates = [
        {"name": "Ho Chi Minh", "lon": 106.666672, "lat": 10.75},
        {"name": "Ha Noi", "lon": 105.841171, "lat": 21.0245},
        {"name": "Da Nang", "lon": 108.220833, "lat": 16.06778}
    ]
    part = "minutely,hourly,daily"
    api_key = os.getenv('OPENWEATHERMAP_API_KEY')
    
    if not api_key:
        logger.error("Không tìm thấy API key của OpenWeatherMap trong biến môi trường")
        return
    
    for coord in coordinates:
        name = coord["name"]
        lat = coord["lat"]
        lon = coord["lon"]
    
        url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&units=metric&exclude={part}&appid={api_key}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            # Thêm tên thành phố vào dữ liệu
            data['name'] = name
            
            # Chuyển đổi mốc thời gian sang múi giờ Việt Nam
            vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
            
            # Chuyển đổi timestamp hiện tại
            dt_utc = datetime.fromtimestamp(data['current']['dt'], tz=timezone.utc)
            dt_vietnam = dt_utc.astimezone(vietnam_tz)
            data['current']['dt'] = dt_vietnam.strftime('%Y-%m-%d %H:%M:%S')
            
            # Chuyển đổi thời gian mặt trời mọc và lặn
            dt_sunrise_utc = datetime.fromtimestamp(data['current']['sunrise'], tz=timezone.utc)
            dt_sunset_utc = datetime.fromtimestamp(data['current']['sunset'], tz=timezone.utc)
            
            dt_sunrise_vietnam = dt_sunrise_utc.astimezone(vietnam_tz)
            dt_sunset_vietnam = dt_sunset_utc.astimezone(vietnam_tz)
            
            data['current']['sunrise'] = dt_sunrise_vietnam.strftime('%Y-%m-%d %H:%M:%S')
            data['current']['sunset'] = dt_sunset_vietnam.strftime('%Y-%m-%d %H:%M:%S')
            
            logger.info(f"Lấy dữ liệu thời tiết thành công cho {name}")
            
            # Gửi dữ liệu đến Kafka
            producer.produce(
                'weather-data-topic',
                key=name.encode('utf-8'),
                value=json.dumps(data).encode('utf-8'),
                callback=delivery_report
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy dữ liệu thời tiết cho {name}: {e}")
        except Exception as e:
            logger.error(f"Lỗi không mong muốn khi lấy dữ liệu thời tiết cho {name}: {e}")
    
    # Đảm bảo tất cả tin nhắn đã được gửi đi
    producer.flush()
    logger.info("Tất cả dữ liệu đã được gửi thành công")

if __name__ == "__main__":
    interval = int(os.getenv('FETCH_INTERVAL_SECONDS', 600))  # Mặc định 10 phút
    
    try:
        while True:
            fetch_and_send_weather_data()
            logger.info(f"Đang chờ {interval} giây trước khi lấy dữ liệu lần tiếp theo.")
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Dừng chương trình Producer")