import json
import time
import requests
from kafka import KafkaProducer

API_KEY = "4eed65b0fb524bb4612070dd9aa8483c"  # replace with your key
CITY = "London"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

KAFKA_BROKER = "kafka:9092"
TOPIC = "weather-data"

def get_weather():
    response = requests.get(URL)
    if response.status_code == 200:
        data = response.json()
        return data.get("main")  # ✅ only keep 'main' section
    else:
        print(f"Error: {response.status_code}")
        return None

def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("🚀 Producer started...")

    while True:
        weather_data = get_weather()
        if weather_data:
            producer.send(TOPIC, weather_data)
            print(f"✅ Sent: {weather_data}")
        time.sleep(10)

if __name__ == "__main__":
    main()
