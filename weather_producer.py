from confluent_kafka import Producer
import json
import requests
from datetime import datetime, timezone
import pytz
import time
import logging
import os
import threading  # Thêm thư viện để chạy luồng riêng
import keyboard  # Thư viện để bắt sự kiện bàn phím
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('weather_producer')

# Load environment variables
load_dotenv()

# Biến toàn cục để kiểm soát thời gian chờ
interrupt_sleep = False

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def fetch_and_send_weather_data():
    """Lấy dữ liệu từ API OpenWeatherMap và gửi đến Kafka"""
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
        logger.error("OpenWeatherMap API key not found in environment variables")
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
            
            # Add city name to data
            data['name'] = name
            
            # Convert timestamps to Vietnam timezone
            vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
            
            # Convert current timestamp
            dt_utc = datetime.fromtimestamp(data['current']['dt'], tz=timezone.utc)
            dt_vietnam = dt_utc.astimezone(vietnam_tz)
            data['current']['dt'] = dt_vietnam.strftime('%Y-%m-%d %H:%M:%S')
            
            # Convert sunrise and sunset timestamps
            dt_sunrise_utc = datetime.fromtimestamp(data['current']['sunrise'], tz=timezone.utc)
            dt_sunset_utc = datetime.fromtimestamp(data['current']['sunset'], tz=timezone.utc)
            
            dt_sunrise_vietnam = dt_sunrise_utc.astimezone(vietnam_tz)
            dt_sunset_vietnam = dt_sunset_utc.astimezone(vietnam_tz)
            
            data['current']['sunrise'] = dt_sunrise_vietnam.strftime('%Y-%m-%d %H:%M:%S')
            data['current']['sunset'] = dt_sunset_vietnam.strftime('%Y-%m-%d %H:%M:%S')
            
            logger.info(f"Fetched weather data for {name}")
            
            # Produce to Kafka
            producer.produce(
                'weather-data-topic',
                key=name.encode('utf-8'),
                value=json.dumps(data).encode('utf-8'),
                callback=delivery_report
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error for {name}: {e}")
    
    # Flush the producer to ensure all messages are sent
    producer.flush()
    logger.info("All messages have been sent")

def listen_for_keypress():
    """
    Luồng riêng để lắng nghe phím Ctrl+R và ngắt thời gian chờ
    """
    global interrupt_sleep
    while True:
        keyboard.wait("ctrl+r")
        interrupt_sleep = True
        logger.info("\nCtrl+R được nhấn. Tiến hành lấy dữ liệu ngay lập tức...")

if __name__ == "__main__":
    interval = int(os.getenv('FETCH_INTERVAL_SECONDS', 600))  # Default 10 minutes
    
    # Chạy luồng lắng nghe sự kiện phím Ctrl+R
    keypress_thread = threading.Thread(target=listen_for_keypress, daemon=True)
    keypress_thread.start()

    try:
        while True:
            fetch_and_send_weather_data()
            logger.info(f"Sleeping for {interval} seconds. Press Ctrl+R to fetch immediately.")
            
            # Reset trạng thái trước khi bắt đầu sleep
            interrupt_sleep = False  
            start_time = time.time()
            
            while time.time() - start_time < interval:
                if interrupt_sleep:
                    logger.info("Đã ngắt thời gian chờ. Tiến hành lấy dữ liệu mới...")
                    break
                time.sleep(0.1)  # Kiểm tra mỗi 0.1 giây
            
    except KeyboardInterrupt:
        logger.info("Producer shutting down")
