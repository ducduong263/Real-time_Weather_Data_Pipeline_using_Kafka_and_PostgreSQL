CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    temperature FLOAT NOT NULL,
    feels_like FLOAT,
    humidity INTEGER NOT NULL,
    pressure INTEGER,
    wind_speed FLOAT,
    wind_deg INTEGER,
    clouds INTEGER,
    weather_condition VARCHAR(100),
    weather_description VARCHAR(100),
    sunrise TIMESTAMP WITHOUT TIME ZONE,
    sunset TIMESTAMP WITHOUT TIME ZONE,
    UNIQUE (city, timestamp)  -- Ràng buộc UNIQUE để tránh dữ liệu trùng lặp
);

-- Tạo index để tăng tốc truy vấn
CREATE INDEX IF NOT EXISTS idx_weather_city_timestamp ON weather(city, timestamp);
