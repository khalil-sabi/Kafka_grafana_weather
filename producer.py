from kafka import KafkaProducer
import requests
from dotenv import load_dotenv, dotenv_values
import os
import time

load_dotenv()

# Kafka broker configuration
bootstrap_servers = os.getenv("BROKER_URL")
location = "paris"
lang = "fr"
api_key = os.getenv("API_KEY")

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: str(v).encode('utf-8'), 
    key_serializer=lambda k: str(k).encode('utf-8') if k else None 
)

topic = 'weather'

# Produce a message to topic
base_api_url = os.getenv("API_URL")
api_url = base_api_url+"v1/current.json"
params = {'q': location, 'lang': lang, 'key': api_key}
while True:
    response = requests.get(api_url, params=params)
    
    if response.status_code == 200:
        try:
            weather_data = response.json()
            print(weather_data)

            producer.send(topic, key=int(time.time()), value=weather_data)
        except requests.exceptions.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
    else:
        print(f"Error: Unable to fetch weather data. Status code: {response.status_code}")
    time.sleep(60)
