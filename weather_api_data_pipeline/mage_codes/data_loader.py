import os
import json
import requests
import pandas as pd
from datetime import datetime
import io

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader, test

# Function to fetch data from API
def fetch_data_from_api(api_url, last_timestamp=None, limit=None):
    params = {}
    if last_timestamp:
        params['since'] = last_timestamp
    if limit:
        params['limit'] = limit

    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data from API for:", api_url)
        return None

# Function to save DataFrame as Parquet file
def save_dataframe_as_parquet(dataframe, filename):
    dataframe.to_parquet(filename)

def get_last_timestamp():
    if os.path.exists('last_timestamp.txt'):
        with open('last_timestamp.txt', 'r') as file:
            last_timestamp = file.read().strip()
        return last_timestamp
    else:
        return None

def save_last_timestamp(timestamp):
    with open('last_timestamp.txt', 'w') as file:
        file.write(timestamp)

def get_last_fetched_city_index():
    if os.path.exists('last_fetched_city_index.txt'):
        with open('last_fetched_city_index.txt', 'r') as file:
            last_index = int(file.read().strip())
        return last_index
    else:
        return 0  # Start from the first city if no index file exists

def save_last_fetched_city_index(index):
    with open('last_fetched_city_index.txt', 'w') as file:
        file.write(str(index))

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Load data from API and return as a DataFrame.
    """
    # Read your DataFrame with city names
    csv_path = '/home/pipeline/citinames.csv'
    city_data = pd.read_csv(csv_path)  # Update with your DataFrame file path
    city_data['City'] = city_data['City'].apply(lambda x: x.split(' (')[0])

    # Specify the target number of records
    target_records = 500

    # Initialize lists to store fetched data and log information
    all_data = []
    failed_cities = []
    num_records_fetched = 0

    # Get the index of the last fetched city
    last_fetched_city_index = get_last_fetched_city_index()

    # Iterate over each city in the DataFrame, starting from the last fetched city index
    for i in range(last_fetched_city_index, len(city_data['City'])):
        city = city_data['City'][i]

        # API URL for the current city
        api_url = f"https://api.weatherapi.com/v1/current.json?key=ef7c40ce8499447eb2b100832240802&q={city}&aqi=no"

        # Fetch data from API
        api_response = fetch_data_from_api(api_url, get_last_timestamp(), limit=25)
        print("Data from API for", city, ":", api_response)

        if api_response:
            # Flatten the nested structure of the API response
            flat_data = {
                'City': city,
                'Location_Name': api_response['location']['name'],
                'Location_Region': api_response['location']['region'],
                'Location_Country': api_response['location']['country'],
                'Location_Lat': api_response['location']['lat'],
                'Location_Lon': api_response['location']['lon'],
                'Location_Tz_Id': api_response['location']['tz_id'],
                'Location_Localtime_Epoch': api_response['location']['localtime_epoch'],
                'Location_Localtime': api_response['location']['localtime'],
                'Current_Last_Updated_Epoch': api_response['current']['last_updated_epoch'],
                'Current_Last_Updated': api_response['current']['last_updated'],
                'Current_Temp_C': api_response['current']['temp_c'],
                'Current_Temp_F': api_response['current']['temp_f'],
                'Current_Is_Day': api_response['current']['is_day'],
                'Current_Condition_Text': api_response['current']['condition']['text'],
                'Current_Condition_Icon': api_response['current']['condition']['icon'],
                'Current_Condition_Code': api_response['current']['condition']['code'],
                'Current_Wind_Mph': api_response['current']['wind_mph'],
                'Current_Wind_Kph': api_response['current']['wind_kph'],
                'Current_Wind_Degree': api_response['current']['wind_degree'],
                'Current_Wind_Dir': api_response['current']['wind_dir'],
                'Current_Pressure_Mb': api_response['current']['pressure_mb'],
                'Current_Pressure_In': api_response['current']['pressure_in'],
                'Current_Precip_Mm': api_response['current']['precip_mm'],
                'Current_Precip_In': api_response['current']['precip_in'],
                'Current_Humidity': api_response['current']['humidity'],
                'Current_Cloud': api_response['current']['cloud'],
                'Current_Feelslike_C': api_response['current']['feelslike_c'],
                'Current_Vis_Km': api_response['current']['vis_km'],
                'Current_Vis_Miles': api_response['current']['vis_miles'],
                'Current_Uv': api_response['current']['uv'],
                'Current_Gust_Kph': api_response['current']['gust_kph']
            }

            all_data.append(flat_data)
            num_records_fetched += 1

            # Update last timestamp for future requests
            latest_timestamp = api_response['current']['last_updated']
            save_last_timestamp(latest_timestamp)

            # Save the index of the last fetched city
            save_last_fetched_city_index(i)

            if num_records_fetched >= target_records:
                break
        else:
            print("No new data fetched from API for", city)
            failed_cities.append(city)

    # Convert fetched data to DataFrame
    df = pd.DataFrame(all_data)

    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

# if __name__ == "__main__":
#     load_data_from_api()
