CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    temperature FLOAT NOT NULL,
    feels_like FLOAT,
    humidity INTEGER NOT NULL,
    pressure INTEGER,
    wind_speed FLOAT,
    uv FLOAT,
    visibility INTEGER,
    clouds INTEGER,
    precipitation FLOAT,  
    weather_condition VARCHAR(100),
    weather_description VARCHAR(100),
    weather_icon VARCHAR(20),
    sunrise TIMESTAMP WITHOUT TIME ZONE,
    sunset TIMESTAMP WITHOUT TIME ZONE,
    UNIQUE (city, timestamp)
);

-- Tạo index để tăng tốc truy vấn
CREATE INDEX IF NOT EXISTS idx_weather_city_timestamp ON weather(city, timestamp);