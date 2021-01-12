"""
Basic functions for getting data from a dWeather gateway via https if you prefer to work
in pandas dataframes rather than Python's built in types. A wrapper for http_client.
"""
from dweather_client.http_client import get_rainfall_dict, get_temperature_dict, RTMAClient, get_station_csv, get_simulated_hurricane_files, get_hurricane_dict
from dweather_client.ipfs_client import cat_station_csv
from dweather_client.df_utils import get_station_ids_with_icao, nearby_storms
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

def get_simulated_hurricane_df(basin, **kwargs):
    """
    return:
        pd.DataFrame containing simulated hurricane data. If given kwargs radius, lat, lon,
        will subset df to only include points within radius in km of the point (lat, lon)
    args:
        basin (str), one of: EP, NA, NI, SI, SP or WP
    """
    files = get_simulated_hurricane_files(basin)
    dfs = [pd.read_csv(f, header=None)[range(10)] for f in files]
    df = pd.concat(dfs).reset_index(drop=True)
    columns = ['year', 'month', 'tc_num', 'time_step', 'basin', 'lat', 'lon', 'min_press', 'max_wind', 'rmw']
    df.columns = columns
    if {'radius', 'lat', 'lon'}.issubset(kwargs.keys()):
        df = nearby_storms(df, kwargs['lat'], kwargs['lon'], kwargs['radius'])
    return df

def get_historical_hurricane_df(basin, **kwargs):
    """
    return:
        pd.DataFrame containing historical hurricane data. If given kwargs radius, lat, lon,
        will subset df to only include points within radius in km of the point (lat, lon)
    args:
        basin (str), one of: AL, CP, EP, SL
    """
    if basin not in {'AL', 'CP', 'EP', 'SL'}:
        raise ValueError("Invalid basin ID")
    hist_dict = get_hurricane_dict()
    features = hist_dict['features']
    df_list = []
    for feature in features:
        hurr_dict = feature['properties']
        hurr_dict['lat'] = feature['geometry']['coordinates'][0]
        hurr_dict['lon'] = feature['geometry']['coordinates'][1]
        df_list.append(hurr_dict)
    df = pd.DataFrame(df_list)
    df = df[df["BASIN"] == basin]
    df['HOUR'] = pd.to_datetime(df["HOUR"])
    if {'radius', 'lat', 'lon'}.issubset(kwargs.keys()):
        df = nearby_storms(df, kwargs['lat'], kwargs['lon'], kwargs['radius'])
    for col in df:
        if col != "HOUR":
            df[col] = pd.to_numeric(df[col], errors='ignore')
    return df

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


def get_station_df(station_id, station_dataset="ghcnd-imputed-daily"):
    """ Get a given station's raw data as a pandas dataframe. """
    df = pd.read_csv(io.StringIO(get_station_csv(station_id, station_dataset=station_dataset)))
    return df.set_index(pd.DatetimeIndex(df['DATE']))


def get_station_rainfall_df(station_id, station_dataset="ghcnd"):
    """ Get full daily rainfall time series from GHCN Station Data. """
    # GHCND imputed does not have rainfall, only temperatures, so change the default
    return get_station_df(station_id, station_dataset=station_dataset)[['PRCP', 'NAME']]
    
    
def get_station_temperature_df(station_id, station_dataset="ghcnd-imputed-daily"):
    """ Get full daily min, max temp time series from GHCN Station Data. """
    return get_station_df(station_id, station_dataset=station_dataset)[['TMIN', 'TMAX']]#, 'NAME']]


def get_station_snow_df(station_id, station_dataset="ghcnd"):
    """ Get full daily snowfall time series from GHCN Station Data in mm. """
    return get_station_df(station_id, station_dataset=station_dataset)[['SNOW', 'NAME']]

