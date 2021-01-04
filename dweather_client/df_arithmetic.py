"""
Some functions for organizing dWeather query results into dataframe formats relevant to Arbol's use cases.
Mainly related to summing and averaging over time.
This module imports pandas and numpy, the arithmetic.py module should not
"""
import numpy as np
import pandas as pd
import geopandas as gpd
from dweather_client.http_client import get_rainfall_dict, get_rev_rainfall_dict, get_heads, get_metadata
from dweather_client.utils import listify_period, get_n_closest_station_ids
from dweather_client.ipfs_client import cat_rainfall_dict, cat_station_df, cat_station_csv, cat_metadata
from dweather_client.df_utils import get_station_ids_with_icao
import ipfshttpclient
import datetime
import logging
import os
from dweather_client.utils import snap_to_grid
from dweather_client.http_client import get_metadata, get_rainfall_dict
from shapely.geometry import Point, Polygon
import ipfshttpclient
from collections import Counter

HISTORICAL_START_YEAR = 1981


def get_polygon_df(shapefile_path, dataset, polygon_names, bounding_box, encoding='UTF-8'):
    """
    Get a dataframe of climate data for a given set of polygons.
    """
    polygons = gpd.read_file(shapefile_path)[['NAME_0', 'NAME_1', 'geometry']]
    polygons.columns = ["country", "state", "geometry"]

    metadata = get_metadata(get_heads()[dataset])

    start = datetime.datetime.strptime(metadata['date range'][0], '%Y/%d/%m')
    end = datetime.datetime.today()
    date_range = pd.date_range(start=start, end=end)
    df = pd.DataFrame([[0 for i in polygon_names] for i in date_range], columns=polygon_names)
    df.insert(0, "Date", date_range, True)
    df.index = df['Date']
    df.drop(['Date'], axis=1, inplace=True)
    df.index.name = None

    for index, row in polygons.iterrows():
        bbox_size = int((bounding_box[1].y - bounding_box[0].y) * (bounding_box[1].x - bounding_box[0].x) / metadata['resolution'] / metadata['resolution'])
        logging.info("Building %s (%i of %i polygons)" % (row['state'], index + 1, len(polygons.index)))
        logging.info("Matching points within a bounding box of %i points." % bbox_size)
        exec_counter = 0
        poly_counter = Counter({})
        for latitr in np.arange(bounding_box[0].y, bounding_box[1].y, metadata['resolution']):
            if ((exec_counter % 1000) == 0):
                logging.info("Scanning point %i of %i" % (exec_counter, bbox_size))
            for lonitr in np.arange(bounding_box[0].x, bounding_box[1].x, metadata['resolution']):
                if Point(lonitr, latitr).within(row['geometry']):
                    slat, slon = snap_to_grid(latitr, lonitr, metadata)
                    logging.info("Found match at (%s, %s), point %i of %i" % ( \
                        "{:.3f}".format(slat),
                        "{:.3f}".format(slon),
                        exec_counter,
                        bbox_size
                    ))
                    try:
                        rain_counter = get_rainfall_dict(slat, slon, dataset, get_counter=True)
                        poly_counter = poly_counter + rain_counter
                    except:
                        logging.warning("Could not retrieve data for (%s, %s)" % ("{:.3f}".format(slat), "{:.3f}".format(slon)))
                        continue
                exec_counter = exec_counter + 1
        for day in poly_counter:
            df.at[day.strftime('%Y-%m-%d'), row['state']] += poly_counter[day]
    return df
 

def cat_station_df_list(station_ids, station_dataset="ghcnd-imputed-daily", pin=True, force_hash=None):
    batch_hash = force_hash
    if (force_hash is None):
        batch_hash = get_heads()[station_dataset]
    metadata = cat_metadata(batch_hash, pin=pin)
    station_content = []
    with ipfshttpclient.connect() as client:
        for station_id in station_ids:
            logging.info("(%i of %i): Loading station %s from %s into DataFrame%s" % ( \
                station_ids.index(station_id) + 1,
                len(station_ids),
                station_id, 
                "dWeather head" if force_hash is None else "forced hash",
                " and pinning to ipfs datastore" if pin else ""
            ))
            try:
                station_content.append(cat_station_df( \
                    station_id,
                    station_dataset=station_dataset,
                    client=client,
                    pin=pin,
                    force_hash=batch_hash
            ))
            except ipfshttpclient.exceptions.ErrorResponse:
                logging.warning("Station %s not found" % station_id)
                
    return station_content 

def cat_icao_stations(station_dataset="ghcnd-imputed-daily", pin=True, force_hash=None):
    """
    For every station that has an icao code, load it into a dataframe and
    return them all as a list.
    """
    station_ids = get_station_ids_with_icao()
    return cat_station_df_list(station_ids, station_dataset=station_dataset, pin=pin, force_hash=force_hash)

def cat_n_closest_station_dfs(lat, lon, n, station_dataset="ghcnd-imputed-daily", pin=True, force_hash=None):
    """
    Load the closest n stations to a given point into a list of dataframes.
    """
    if (force_hash is None):
        metadata = cat_metadata(get_heads()[station_dataset])
    else:
        metadata = cat_metadata(force_hash)
    station_ids = get_n_closest_station_ids(lat, lon, metadata, n)
    return cat_station_df_list(station_ids, station_dataset=station_dataset, pin=pin, force_hash=force_hash)

def sum_period_df(df, ps, pe, yrs, peril):
    """ 
    Get the sums of a peril by a defined period.
    
    return:
        pd.DataFrame indexed by year
        
    args:
        df = weather data with a daily index
        ps = contract start period, datetime.date object
        pe = contract end period, datetime.date object
        yrs = years lookback
        peril = the name of column you wish to sum
        
    """
    newdf = df
    years = np.array([])
    sums = np.array([])

    for year in range(ps.year-yrs, ps.year):
        if ps.year < pe.year: # if period crosses jan 1 into a new year
            left = str(year)+'-'+str(ps.month)+'-'+str(ps.day)
            right = str(year+1)+'-'+str(pe.month)+'-'+str(pe.day)
        else:
            left = str(year)+'-'+str(ps.month)+'-'+str(ps.day)
            right = str(year)+'-'+str(pe.month)+'-'+str(pe.day)

        sumChunk = newdf[left:right]
        years = np.append(years, int(year))
        sums = np.append(sums, sumChunk[peril].sum())
    return pd.DataFrame({'year': years, 'value': sums}).set_index("year")


def avg_period_df(df, ps, pe, yrs, peril):
    """ 
    Gets the avg of a peril by a defined period
    
    return:
        pd.DataFrame indexed by year
        
    args:
        df = weather data with a daily index
        ps = contract start period, datetime.date object
        pe = contract end period, datetime.date object
        yrs = years lookback
        peril = the name of column you wish to avg
        
    """

    newdf = df
    years = np.array([])
    avgs = np.array([])

    for year in range(ps.year-yrs, ps.year):
        if ps.year < pe.year: # if period crosses jan 1 into a new year
            left = str(year)+'-'+str(ps.month)+'-'+str(ps.day)
            right = str(year+1)+'-'+str(pe.month)+'-'+str(pe.day)
        else:
            left = str(year)+'-'+str(ps.month)+'-'+str(ps.day)
            right = str(year)+'-'+str(pe.month)+'-'+str(pe.day)

        sumChunk = newdf[left:right]
        years = np.append(years,int(year))
        avgs = np.append(avgs, sumChunk[peril].mean())
    return pd.DataFrame({'year': years, 'value': avgs}).set_index('year')
