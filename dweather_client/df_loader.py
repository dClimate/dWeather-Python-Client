"""
Basic functions for getting data from a dWeather gateway via https if you prefer to work
in pandas dataframes rather than Python's built in types. A wrapper for http_client.
"""
from dweather_client.http_client import get_rainfall_dict, get_temperature_dict,\
    RTMAClient, get_station_csv, get_simulated_hurricane_files, get_hurricane_dict, get_ibracs_hurricane_file, get_era5_dict
from dweather_client.ipfs_client import cat_station_csv
from dweather_client.df_utils import get_station_ids_with_icao, nearby_storms, boxed_storms
import pandas as pd
import numpy as np
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

def get_era5_wind_speed_df(lat, lon):
    """
    Get era5 time series df containing wind-u, wind-v, and total windspeed calculated
    with the pythagorean theorem. 

    return:
        pd.DataFrame with pd.datetime index in hours
    
    args:
        lat: float latitude that is over land (throws exception if invalid for era5)
        lon: float longitude that is over land (throws exception if invalid for era5)
    """
    snapped_lat_lon, df_u = get_era5_df(lat, lon, 'era5_land_wind_u-hourly')
    df_v = get_era5_df(lat, lon, 'era5_land_wind_v-hourly')[1]

    res = pd.DataFrame()
    res['wind_u'] = df_u.VALUE
    res['wind_v'] = df_v.VALUE

    # index the data to the smaller of the two datasets in the event one has updated and the other has not
    res_index = df_v.index if len(df_v) < len(df_u) else df_u.index
    res.set_index(res_index)

    res['wind_speed'] = np.hypot(res.wind_u, res.wind_v)

    return snapped_lat_lon, res

def get_era5_df(lat, lon, dataset):
    """
    Get era5 time series df

    return:
        pd.DataFrame with pd.datetime index in hours
    
    args:
        lat: float latitude that is over land (throws exception if invalid for era5)
        lon: float longitude that is over land (throws exception if invalid for era5)
        dataset: str currently only 'era5_land_wind_u-hourly'. More to come
    """
    snapped_lat_lon, era5_dict = get_era5_dict(lat, lon, dataset)
    era5_dict = {"DATE": [k for k in era5_dict.keys()], "VALUE": [v for v in era5_dict.values()]}
    era5_df = pd.DataFrame.from_dict(era5_dict)
    era5_df.DATE = pd.to_datetime(era5_df.DATE)
    return snapped_lat_lon, era5_df.set_index(['DATE'])

def get_simulated_hurricane_df(basin, **kwargs):
    """
    return:
        pd.DataFrame containing simulated hurricane data. If given kwargs radius, lat, lon,
        will subset df to only include points within radius in km of the point (lat, lon).
        Otherwise, if given kwargs 'min_lat', 'min_lon', 'max_lat', 'max_lon', selects points within
        a bounding box.
    args:
        basin (str), one of: EP, NA, NI, SI, SP or WP
    """
    files = get_simulated_hurricane_files(basin)
    dfs = [pd.read_csv(f, header=None)[range(10)] for f in files]
    df = pd.concat(dfs).reset_index(drop=True)
    columns = ['year', 'month', 'tc_num', 'time_step', 'basin', 'lat', 'lon', 'min_press', 'max_wind', 'rmw']
    df.columns = columns
    df.loc[df.lon > 180, 'lon'] = df.lon - 360
    if {'radius', 'lat', 'lon'}.issubset(kwargs.keys()):
        df = nearby_storms(df, kwargs['lat'], kwargs['lon'], kwargs['radius'])
    elif {'min_lat', 'min_lon', 'max_lat', 'max_lon'}.issubset(kwargs.keys()):
        df = boxed_storms(df, kwargs['min_lat'], kwargs['min_lon'], kwargs['max_lat'], kwargs["max_lon"])
    
    return df

def get_atcf_hurricane_df(basin, **kwargs):
    """
    return:
        pd.DataFrame containing ATCF historical hurricane data. If given kwargs radius, lat, lon,
        will subset df to only include points within radius in km of the point (lat, lon)
        Otherwise, if given kwargs 'min_lat', 'min_lon', 'max_lat', 'max_lon', selects points within
        a bounding box.
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
    elif {'min_lat', 'min_lon', 'max_lat', 'max_lon'}.issubset(kwargs.keys()):
        df = boxed_storms(df, kwargs['min_lat'], kwargs['min_lon'], kwargs['max_lat'], kwargs["max_lon"])

    for col in df:
        if col != "HOUR":
            df[col] = pd.to_numeric(df[col], errors='ignore')
    return df

def get_historical_hurricane_df(basin, **kwargs):
    """
    return:
        pd.DataFrame containing ibtracs historical hurricane data. More comprehensive than get_atcf_hurricane_df,
        but less frquently updated. If given kwargs radius, lat, lon,
        will subset df to only include points within radius in km of the point (lat, lon)
        Otherwise, if given kwargs 'min_lat', 'min_lon', 'max_lat', 'max_lon', selects points within
        a bounding box.
    args:
        basin (str), one of: 'NI', 'SI', 'NA', 'EP', 'WP', 'SP', 'SA'
    """
    df = pd.read_csv(get_ibracs_hurricane_file(), na_values=["", " "], keep_default_na=False, low_memory=False)
    df = df[1:]
    df = df[df['BASIN'] == basin]

    df["lat"] = df.LAT.astype(float)
    df["lon"] = df.LON.astype(float)

    del df["LAT"]
    del df["LON"]

    if {'radius', 'lat', 'lon'}.issubset(kwargs.keys()):
        df = nearby_storms(df, kwargs['lat'], kwargs['lon'], kwargs['radius'])
    elif {'min_lat', 'min_lon', 'max_lat', 'max_lon'}.issubset(kwargs.keys()):
        df = boxed_storms(df, kwargs['min_lat'], kwargs['min_lon'], kwargs['max_lat'], kwargs["max_lon"])
    
    df["HOUR"] = pd.to_datetime(df["ISO_TIME"])
    del df["ISO_TIME"]

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

