if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Define the mapping of old column names to new column names
    column_mapping = {
        'location_name': 'name',
        'location_region': 'region',
        'location_country': 'country',
        'location_lat': 'latitude',
        'location_lon': 'longitude',
        'location_tz_id': 'timezone_id',
        'location_localtime_epoch': 'localtime_epoch',
        'location_localtime': 'localtime',
        'current_last_updated_epoch': 'last_updated_epoch',
        'current_last_updated': 'last_updated',
        'current_temp_c': 'temp_c',
        'current_temp_f': 'temp_f',
        'current_is_day': 'is_day',
        'current_condition_text': 'condition_text',
        'current_condition_icon': 'condition_icon',
        'current_condition_code': 'condition_code',
        'current_wind_mph': 'wind_mph',
        'current_wind_kph': 'wind_kph',
        'current_wind_degree': 'wind_degree',
        'current_wind_dir': 'wind_dir',
        'current_pressure_mb': 'pressure_mb',
        'current_pressure_in': 'pressure_in',
        'current_precip_mm': 'precip_mm',
        'current_precip_in': 'precip_in',
        'current_humidity': 'humidity',
        'current_cloud': 'cloud',
        'current_feelslike_c': 'feelslike_c',
        'current_vis_km': 'vis_km',
        'current_vis_miles': 'vis_miles',
        'current_uv': 'uv',
        'current_gust_kph': 'gust_kph'
    }

    # Rename columns using the mapping
    data.rename(columns=column_mapping, inplace=True)

    return data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
