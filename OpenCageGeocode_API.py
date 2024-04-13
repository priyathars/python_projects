""" CODE WITHOUT MULTI-THREADING CONCEPT """

import pandas as pd
from opencage.geocoder import OpenCageGeocode
from configparser import ConfigParser
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys

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

required_column = ["Address / City / State / Zip"]

print("Initializing the Geo Locator Program")

try:

    key = "e3e939a17ce64e2bb0d438d388621cbd"
    geocoder = OpenCageGeocode(key)

    print("API Started to Requesting")

    config = ConfigParser()
    config.read('Properties.ini')
    AddressInput = config['Address_File']['Input_Address_Filepath']
    AddressOutput = config['Address_File']['Output_Address_Filepath']

    print("Check 1 - Input File checking")

    if not os.path.isfile(AddressInput):  # Condition to check Input File Existence.
        logger.error("No input file provided.")
        print("No input file provided!")
        sys.exit(1)  # if file not exist it will exit

    else:  # Condition if file Exist then it will run
        address_df = pd.read_excel(AddressInput)  # Read all the data from excel file

        missing_column = set(required_column) - set(address_df.columns)  # missing col to verify the required col

        if missing_column:
            logger.error(f"Missing required columns in the input file: {', '.join(missing_column)}")
            print(f"Missing the required column in input file Address / City / State / Zip")
            sys.exit(1)

        print("Reading the Input Excel File")

    print("Check 2 - Taking the Address from the Input File")

    latitude = []
    longitude = []
    # we create empty lists to hold the latitude and longitude of the geocoded addresses

    for add in address_df["Address / City / State / Zip"]:  # iterate the address rows each time to get the co-ordinates
        result = geocoder.geocode(add, no_annotations="1")  # no_annotations parameter take only lat and long.

        if result and len(result):  # this condition append the lat and long to each address rows.
            longitude.append(result[0]["geometry"]["lng"])
            latitude.append(result[0]["geometry"]["lat"])
        #            print("Inserting the Latitude and Longitude")

        else:  # condition become false then take lat and long output has N/A
            longitude.append("N/A")
            latitude.append("N/A")

    address_df["latitude"] = latitude
    address_df["longitude"] = longitude
    address_df.to_excel(AddressOutput)  # DataFrame is written to an output Excel file specified in AddressOutput
    # using to_excel()
    print("Latitude and Longitude Inserted,Kindly check your output Excel File")
    logger.info("File is created and Co-Ordinates are loaded to the corresponding columns")
    user_input = input("Press Enter Key to exit: ")  # Terminal Command to exit after completion

except FileNotFoundError as e:
    error_message = f"File not found: {e}"
    logger.error(error_message)
    print(error_message, flush=True)
except Exception as e:
    error_message = f"An error occurred: {str(e)}"
    logger.error(error_message)
    print(error_message)
