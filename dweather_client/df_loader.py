"""
Basic functions for getting data from a dWeather gateway via https if you prefer to work
in pandas dataframes rather than Python's built in types. A wrapper for http_client.
"""
from http_client import get_rainfall_dict, get_station_csv
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