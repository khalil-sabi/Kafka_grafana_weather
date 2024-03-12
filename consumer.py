from kafka import KafkaConsumer
import os
from dotenv import load_dotenv
import snowflake.connector
import json

load_dotenv()

warehouse = "DENSITE"
database = "DENSITE"

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'

# Create consumer instance
consumer = KafkaConsumer(
    'weather',
    bootstrap_servers=bootstrap_servers,
    group_id='your_consumer_group', 
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

con = snowflake.connector.connect(
    user=os.getenv("SNOWSQL_USR"),
    password=os.getenv("SNOWSQL_PWD"),
    account=os.getenv("SNOWSQL_ACT"),
    warehouse=warehouse,
    database=database
)
cur = con.cursor()
# Start consuming messages
for element in consumer:
    # Print the message
    message = json.loads(element.value.replace("'", '"'))
    print(f"Key: {element.key}, Value: {element.value}, Offset: {element.offset}")

    # extract values from the message And inserting them into database
    location_name = message['location']['name']
    region = message['location']['region']
    country = message['location']['country']
    latitude = message['location']['lat']
    longitude = message['location']['lon']
    timezone_id = message['location']['tz_id']
    created_epoch = message['location']['localtime_epoch']
    created = message['location']['localtime']
    last_updated_epoch = message['current']['last_updated_epoch']
    last_updated = message['current']['last_updated']
    temp_c = message['current']['temp_c']
    temp_f = message['current']['temp_f']
    is_day = message['current']['is_day']
    condition_text = message['current']['condition']['text']
    condition_icon = message['current']['condition']['icon']
    condition_code = message['current']['condition']['code']
    wind_mph = message['current']['wind_mph']
    wind_kph = message['current']['wind_kph']
    wind_degree = message['current']['wind_degree']
    wind_dir = message['current']['wind_dir']
    pressure_mb = message['current']['pressure_mb']
    pressure_in = message['current']['pressure_in']
    precip_mm = message['current']['precip_mm']
    precip_in = message['current']['precip_in']
    humidity = message['current']['humidity']
    cloud = message['current']['cloud']
    feelslike_c = message['current']['feelslike_c']
    feelslike_f = message['current']['feelslike_f']
    vis_km = message['current']['vis_km']
    vis_miles = message['current']['vis_miles']
    uv = message['current']['uv']
    gust_mph = message['current']['gust_mph']
    gust_kph = message['current']['gust_kph']
    cur.execute(
    """
    INSERT INTO weather_data
    (location_name, region, country, latitude, longitude, timezone_id,
    created_epoch, created, last_updated_epoch, last_updated, temp_c, temp_f, is_day,
    condition_text, condition_icon, condition_code, wind_mph, wind_kph, wind_degree,
    wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud,
    feelslike_c, feelslike_f, vis_km, vis_miles, uv, gust_mph, gust_kph)
    VALUES
    (%(location_name)s, %(region)s, %(country)s, %(latitude)s, %(longitude)s, %(timezone_id)s,
    %(created_epoch)s, %(created)s, %(last_updated_epoch)s, %(last_updated)s, %(temp_c)s, %(temp_f)s, %(is_day)s,
    %(condition_text)s, %(condition_icon)s, %(condition_code)s, %(wind_mph)s, %(wind_kph)s, %(wind_degree)s,
    %(wind_dir)s, %(pressure_mb)s, %(pressure_in)s, %(precip_mm)s, %(precip_in)s, %(humidity)s, %(cloud)s,
    %(feelslike_c)s, %(feelslike_f)s, %(vis_km)s, %(vis_miles)s, %(uv)s, %(gust_mph)s, %(gust_kph)s)
    """,
    {
        'location_name': location_name,
        'region': region,
        'country': country,
        'latitude': latitude,
        'longitude': longitude,
        'timezone_id': timezone_id,
        'created_epoch': created_epoch,
        'created': created,
        'last_updated_epoch': last_updated_epoch,
        'last_updated': last_updated,
        'temp_c': temp_c,
        'temp_f': temp_f,
        'is_day': is_day,
        'condition_text': condition_text,
        'condition_icon': condition_icon,
        'condition_code': condition_code,
        'wind_mph': wind_mph,
        'wind_kph': wind_kph,
        'wind_degree': wind_degree,
        'wind_dir': wind_dir,
        'pressure_mb': pressure_mb,
        'pressure_in': pressure_in,
        'precip_mm': precip_mm,
        'precip_in': precip_in,
        'humidity': humidity,
        'cloud': cloud,
        'feelslike_c': feelslike_c,
        'feelslike_f': feelslike_f,
        'vis_km': vis_km,
        'vis_miles': vis_miles,
        'uv': uv,
        'gust_mph': gust_mph,
        'gust_kph': gust_kph
    }
    )

