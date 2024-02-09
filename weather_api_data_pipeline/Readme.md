In this project i have created a pipeline that the received from the weather api which is in. json format.

I want to extract the data from the api and store the data in the Datalake(or locally) in the .parquet format.

Now I want to extract that data which is stored in .parquet file make some transformations and upload it to Postgres.

Now with the help of mage_ai(orchestration tool) to automate the process for every 5 minutes.

check the workbook.ipynb for code clarifications

## data from weather api
data = {
    'location': {
        'name': 'New Delhi',
        'region': 'Delhi',
        'country': 'India',
        'lat': 28.6,
        'lon': 77.2,
        'tz_id': 'Asia/Kolkata',
        'localtime_epoch': 1707395242,
        'localtime': '2024-02-08 17:57'
    },
    'current': {
        'last_updated_epoch': 1707394500,
        'last_updated': '2024-02-08 17:45',
        'temp_c': 19.0,
        'temp_f': 66.2,
        'is_day': 1,
        'condition': {
            'text': 'Mist',
            'icon': '//cdn.weatherapi.com/weather/64x64/day/143.png',
            'code': 1030
        },
        'wind_mph': 11.9,
        'wind_kph': 19.1,
        'wind_degree': 270,
        'wind_dir': 'W',
        'pressure_mb': 1016.0,
        'pressure_in': 30.0,
        'precip_mm': 0.0,
        'precip_in': 0.0,
        'humidity': 37,
        'cloud': 25,
        'feelslike_c': 19.0,
        'vis_km': 4.0,
        'vis_miles': 2.0,
        'uv': 4.0,
        'gust_kph': 26.2
    }
}

It has nested columns so we have to take care of the data_set.check the workbook.ipynb

using of limit in the code explanantion 

## Limit:

The limit parameter is used to specify the maximum number of records to be fetched in a single API call.
It's useful for paginating through large datasets or controlling the amount of data retrieved in each request.
When the API supports pagination, you can use the limit parameter along with other parameters like offset or page to retrieve data in manageable chunks.
For example, if an API has 10,000 records and you set the limit to 100, each API call will fetch 100 records, and you would need to make 100 calls to retrieve all the data.

## Total Records:

The total_records parameter represents the total number of records you want to fetch from the API.
It's used to define the overall scope of data retrieval.
Unlike the limit parameter, which controls the number of records fetched per call, the total_records parameter determines the endpoint at which data fetching should stop.
For example, if you need to fetch 2000 records from an API, you would continue making API calls until you have retrieved a total of 2000 records, regardless of how many records are fetched in each call.
In summary, the limit parameter controls the number of records fetched per API call, while the total_records parameter defines the total number of records you want to fetch in total. The combination of these two parameters helps in fetching data in manageable batches until the desired total number of records is reached.

## using "yield" in the code

## using 'len'
If len(city_data['City']) accurately retrieves the length of larger datasets, then the code will function as expected. The purpose of len(city_data['City']) is to ensure that the loop iterates over all the cities in the 'City' column of the DataFrame, regardless of the dataset's size.

However, it's essential to consider the performance implications of working with large datasets. Retrieving the length of a DataFrame column (len(city_data['City'])) can be computationally expensive for very large datasets because it involves scanning the entire column. In such cases, it may be more efficient to use other approaches to determine the range of the loop, such as using the .index attribute or .shape property of the DataFrame. These approaches provide the number of rows directly without iterating over the entire column.

For example:

python
Copy code
num_cities = city_data.shape[0]  # Get the number of rows (cities) directly
for i in range(last_fetched_city_index, num_cities):
    city = city_data['City'][i]
    # Continue with the loop logic
Using these methods can improve the performance of your code when working with large datasets. However, if the dataset is small or the performance impact is negligible, using len(city_data['City']) is perfectly fine and provides a straightforward way to ensure that all cities are processed in the loop.