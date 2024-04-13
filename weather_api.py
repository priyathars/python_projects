import logging

from Database_connection import MysqlDB
import requests
import json
import datetime
from configparser import ConfigParser
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(funcName)s : %(lineno)d : %(message)s',
    handlers=[
        logging.handlers.TimedRotatingFileHandler(
            filename=r"Runtime_log.log",
            when="D",
            interval=1,
            backupCount=0,
        )
    ]
)

# Getting List of Cities in the USA from JSON FILE
with open("city_list.json") as json_file:
    cities = json.load(json_file)

print('Initializing the Weather API Program')


def process_api():
    insert_records = []  # creating empty list to hold the Weather records
    try:
        config = ConfigParser()
        config.read('Properties.ini')
        API_KEY = config['API_File']['key']
        logger.info("API starting to request")
        for city in cities:
            url = "https://api.openweathermap.org/data/2.5/weather?q={},US&appid={}".format(
                city, API_KEY)
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            city_name = data['name']
            temp = data['main']['temp']
            timezone = data['timezone']
            sunrise_time = datetime.datetime.utcfromtimestamp(data['sys']['sunrise'] + int(timezone)).strftime(
                '%H:%M:%S')
            sunset_time = datetime.datetime.utcfromtimestamp(data['sys']['sunset'] + int(timezone)).strftime('%H:%M:%S')
            wind_speed = data['wind']['speed']
            visibility = data['visibility']
            weather = data['weather'][0]['description']
            latitude = data['coord']['lat']
            longitude = data['coord']['lon']

            weather_data = [city_name, temp, timezone, sunrise_time, sunset_time, wind_speed, visibility, weather,
                            latitude, longitude]
            insert_records.append(weather_data)

        logger.info("Inserting weather records to the database")

        if insert_records:  # Condition to check data Existence.
            con = MysqlDB.getConnection()
            cur = con.cursor()

            for insert_record in insert_records:
                cur.execute(
                    "INSERT INTO Weather_Records(City_Name, Temperature, Timezone, Sunrise, Sunset, Wind_Speed, "
                    "Visibility, Weather,Latitude,Longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    insert_record)
                logger.info("Records Insert request received Successfully")
            con.commit()
            con.close()

            logger.info("Inserted Successfully!")
            print("Records Inserted Successfully!")

        else:
            print("No Weather data found in the API response.")
            logger.error("No Weather data found in the API response.")

    except requests.exceptions.RequestException as e:
        error_message = f"Error occurred during API request: {e}"
        logger.error(error_message)
        print(error_message, flush=True)

    except (KeyError, ValueError) as e:
        error_message = f"Error occurred while processing API response: {e}"
        logger.error(error_message)
        print(error_message, flush=True)

    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        logger.error(error_message)
        print(error_message, flush=True)


process_api()
