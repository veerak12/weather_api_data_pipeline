import os
import requests
import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(filename='D:/data_zoomcamp_2024/pipeline/logs/api_data.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        logging.error("Failed to fetch data from API for: %s", api_url)
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

def main():
    # Read your DataFrame with city names
    city_data = pd.read_csv('citinames.csv')  # Update with your DataFrame file path
    city_data['City'] = city_data['City'].apply(lambda x: x.split(' (')[0])
    
    # Specify the target number of records
    target_records = 50

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
        api_response = fetch_data_from_api(api_url, get_last_timestamp(), limit=10)
        logging.info("Data from API for %s: %s", city, api_response)

        if api_response:
            # Add fetched data to the list
            #all_data.extend(api_response)
            all_data.append(api_response)
            num_records_fetched += 1
            
            # Update last timestamp for future requests
            latest_timestamp = api_response['current']['last_updated']
            save_last_timestamp(latest_timestamp)

            # Save the index of the last fetched city
            save_last_fetched_city_index(i)

            if num_records_fetched >= target_records:
                break
        else:
            logging.warning("No new data fetched from API for %s", city)
            failed_cities.append(city)

    # Convert fetched data to DataFrame
    df = pd.DataFrame(all_data)

    # Flatten DataFrame and save as Parquet file
    flattened_data = []
    for data in all_data:
        flat_data = {
            'location_name': data['location']['name'],
            'location_region': data['location']['region'],
            'location_country': data['location']['country'],
            'location_lat': data['location']['lat'],
            'location_lon': data['location']['lon'],
            'location_tz_id': data['location']['tz_id'],
            'location_localtime_epoch': data['location']['localtime_epoch'],
            'location_localtime': data['location']['localtime'],
            'current_last_updated_epoch': data['current']['last_updated_epoch'],
            'current_last_updated': data['current']['last_updated'],
            'current_temp_c': data['current']['temp_c'],
            'current_temp_f': data['current']['temp_f'],
            'current_is_day': data['current']['is_day'],
            'current_condition_text': data['current']['condition']['text'],
            'current_condition_icon': data['current']['condition']['icon'],
            'current_condition_code': data['current']['condition']['code'],
            'current_wind_mph': data['current']['wind_mph'],
            'current_wind_kph': data['current']['wind_kph'],
            'current_wind_degree': data['current']['wind_degree'],
            'current_wind_dir': data['current']['wind_dir'],
            'current_pressure_mb': data['current']['pressure_mb'],
            'current_pressure_in': data['current']['pressure_in'],
            'current_precip_mm': data['current']['precip_mm'],
            'current_precip_in': data['current']['precip_in'],
            'current_humidity': data['current']['humidity'],
            'current_cloud': data['current']['cloud'],
            'current_feelslike_c': data['current']['feelslike_c'],
            'current_vis_km': data['current']['vis_km'],
            'current_vis_miles': data['current']['vis_miles'],
            'current_uv': data['current']['uv'],
            'current_gust_kph': data['current']['gust_kph']
        }
        flattened_data.append(flat_data)

    flat_df = pd.DataFrame(flattened_data)

    # Specify directory for storing Parquet file
    storage_directory = "D:/data_zoomcamp_2024/pipeline/api_data/"

    # Get current date
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Define filename for Parquet file with current date and directory path
    filename = storage_directory + f"data_{current_date}.parquet"

    # Save DataFrame as Parquet file
    save_dataframe_as_parquet(flat_df, filename)

    print("Data saved as Parquet file:", filename)

    
    # Save log information for failed cities
    if failed_cities:
        with open('failed_cities_log.txt', 'w') as file:
            file.write("\n".join(failed_cities))

if __name__ == "__main__":
    main()
