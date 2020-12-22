"""
Basic functions for getting data from a dWeather gateway via https if you prefer to work
in pandas dataframes rather than Python's built in types. A wrapper for http_client.
"""
from dweather_client.http_client import get_rainfall_dict, get_temperature_dict, RTMAClient, get_station_csv
from dweather_client.ipfs_client import cat_station_csv
from dweather_client.df_utils import get_station_ids_with_icao
import pandas as pd
import io
import ipfshttpclient

class RTMADFClient(RTMAClient):
    def get_best_rtma_df(self, lat, lon):
        """
        RTMA precipitation.

        Get a dataframe of for the closest valid rtma grid pair for a
        given lat lon.

        Returns a dataframe indexed on hourly datetime objects.
        """
        snapped_lat_lon, rtma_dict = self.get_best_rtma_dict(lat, lon)
        rtma_dict = {"DATE": [k for k in rtma_dict.keys()], "PRCP": [k for k in rtma_dict.values()]}
        rtma_df = pd.DataFrame.from_dict(rtma_dict)
        rtma_df.DATE = pd.to_datetime(rtma_df.DATE)
        return snapped_lat_lon, rtma_df.set_index(['DATE'])

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

