import requests
from Database_connection import MysqlDB
from datetime import datetime

API_KEY = 'fca_live_8li3jCq6pMmcezkU2WkBovSdluj31zY2JwpEXoTt'
API_URL = 'https://api.freecurrencyapi.com/v1/latest'


def execute_api(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        currency_data = data.get('data', {})
        base_cur = 'USD'
        date = datetime.now().strftime('%Y-%m-%d')

        print("Currencies and Dates are obtained from json body and make it as the record")

        if currency_data:
            con = MysqlDB.getConnection()
            cur = con.cursor()
            insert_data = [(date, base_cur, code, rate) for code, rate in currency_data.items()]
            # print(insert_data)

            print("Received data and proceed to insert")

            cur.executemany("INSERT INTO EXCHANGE_RATES (EXCHANGE_DATE,FROM_CURRENCY,TO_CURRENCY,EXCHANGE_RATE) "
                            "VALUES (%s,%s,%s,%s)", insert_data)
            con.commit()
            con.close()

            print("Inserted Successfully")

        else:
            print("No currency data found in the API response.")

    except requests.exceptions.RequestException as e:
        print("Error occurred during API request:", e)

    except (KeyError, ValueError) as e:
        print("Error occurred while processing API response:", e)

    except Exception as e:
        print("An unexpected error occurred:", e)


execute_api(API_URL + '?apikey=' + API_KEY)
