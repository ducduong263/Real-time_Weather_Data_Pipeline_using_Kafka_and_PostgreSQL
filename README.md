# 🌦️ Real-Time Weather Data Pipeline

A real-time **data pipeline** system using Kafka, Python, PostgreSQL, and Power BI to collect, stream, store, and visualize weather data from the OpenWeatherMap API.

## 📌 Project Summary

This system collects weather data from the OpenWeatherMap API for Vietnamese cities (Hanoi, Ho Chi Minh City, Da Nang), streams it via Kafka, processes and stores it in PostgreSQL, and finally visualizes it through an interactive Power BI dashboard for near real-time weather monitoring.

---

## 🔁 Data Flow

1. **Producer** calls the OpenWeatherMap API to periodically fetch weather data.
2. Weather data is streamed to **Kafka** using the pub/sub model.
3. **Consumer** subscribes to Kafka topic, processes messages, and stores data in **PostgreSQL**.
4. **Power BI** connects to PostgreSQL and visualizes data in real time.

---

## Key Components

- **weather_producer.py**: Fetches data from OpenWeatherMap API and sends it to Kafka
- **weather_consumer.py**: Receives data from Kafka and stores it in PostgreSQL
- **PostgreSQL**: Stores the processed weather data
- **Kafka & Zookeeper**: Message queue system for real-time data processing
- **Power BI**: Creates interactive dashboards for weather data visualization

---

## Installation and Usage

### Requirements

- Docker Desktop
- [OpenWeatherMap](https://openweathermap.org/api) account for API key (One Call API 3.0 in this project) 
- Power BI Desktop (for dashboard development)

### Configuration

Replace `enter-your-api-key` in `.env` with your OpenWeatherMap API key (or you can use my api: 1bef61e5d3e122984de2d9b700f1e7f7)

### Running the Application

```bash
# Start all services
docker-compose up -d
```

### Stopping the Application

```bash
docker-compose down
```
---

## Technical Details

### Python Libraries Used

- **confluent-kafka**: Kafka interaction
- **psycopg2**: PostgreSQL connection
- **requests**: OpenWeatherMap API calls
- **pandas**: Data processing
- **python-dotenv**: Environment variable management
- **pytz**: Timezone handling

### Kafka Configuration

The system uses Kafka as a reliable message broker, enabling real-time data processing with high scalability.

### Data Formatting

Weather data from the OpenWeatherMap API is transformed into an appropriate format before being stored in PostgreSQL, with timezone conversions to display in Vietnam time (UTC+7).

---

## 📊 Data Visualization with Power BI

The project includes a Power BI dashboard for real-time weather monitoring with the following features:

- Current weather conditions visualization
- Temperature and "feels like" temperature indicators
- UV index with low/high scale indicator
- Humidity percentage with visual gauge
- Atmospheric pressure in hPa
- Wind speed in km/h
- Visibility in kilometers
- Sunrise and sunset times
- Weather condition icons and descriptions

![Real-Time Weather Dashboard](https://github.com/ducduong263/Real-time_Weather_Data_Pipeline_using_Kafka_and_PostgreSQL/blob/main/Img/Dashboard/HCM.PNG)
