"""
Basic functions for getting data from a dWeather gateway via https if you prefer to work
in pandas dataframes rather than Python's built in types. A wrapper for http_client.
"""
from dweather_client.http_client import get_rainfall_dict, get_temperature_dict, get_station_csv
import pandas as pd
import io

def get_rainfall_df(lat, lon, dataset):
    """
    Get full daily rainfall time series from cpc, prism, or chirps in mm.
    
    return:
        pd.DataFrame loaded with all daily rainfall
    
    args:
        lat: integer rounded to 3 decimals
        lon: integer rounded to 3 decimals
        dataset = SUPPORTED_DATASETS[n] where n is the index of the dataset you want to use
    """
    
    rainfall_dict = get_rainfall_dict(lat, lon, dataset)
    rainfall_dict = {"DATE": [k for k in rainfall_dict.keys()], "PRCP": [k for k in rainfall_dict.values()]}
    
    rainfall_df = pd.DataFrame.from_dict(rainfall_dict)
    rainfall_df.DATE = pd.to_datetime(rainfall_df.DATE)
    
    return rainfall_df.set_index(['DATE'])


def get_temperature_df(lat, lon, dataset_revision):
    """
    Get full temperature data from one of the temperature datasets
    Args:
        lat: float to 3 decimals
        lon: float to 3 decimals
        dataset_revision: the name of the dataset as listed on the ipfs gateway
    Returns:
        a pandas DataFrame with cols DATE, HIGH and LOW
    """
    highs, lows = get_temperature_dict(lat, lon, dataset_revision)
    intermediate_dict = {
        "DATE": [date for date in highs],
        "HIGH": [highs[date] for date in highs],
        "LOW": [lows[date] for date in lows]
    }
    temperature_df = pd.DataFrame.from_dict(intermediate_dict)
    temperature_df.DATE = pd.to_datetime(temperature_df.DATE)

    return  temperature_df.set_index(["DATE"])


def get_station_df(station_id):
    """ Get a given station's raw data as a pandas dataframe. """
    df = pd.read_csv(io.StringIO(get_station_csv(station_id)))
    return df.set_index(pd.DatetimeIndex(df['DATE']))


def get_station_rainfall_df(station_id):
    """ Get full daily rainfall time series from GHCN Station Data. """
    return get_station_df(station_id)[['PRCP', 'NAME']]
    
    
def get_station_temperature_df(station_id):
    """ Get full daily min, max temp time series from GHCN Station Data. """
    
    return get_station_df(station_id)[['TMIN', 'TMAX', 'NAME']]


def get_station_snow_df(station_id):
    """ Get full daily snowfall time series from GHCN Station Data in mm. """
    return get_station_df(station_id)[['SNOW', 'NAME']]
