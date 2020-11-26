"""
Some functions for organizing dWeather query results into dataframe formats relevant to Arbol's use cases.
Mainly related to summing and averaging over time.
This module imports pandas and numpy, the arithmetic.py module should not
"""
import numpy as np
import pandas as pd
from dweather_client.http_client import get_rainfall_dict, get_rev_rainfall_dict, get_heads, get_metadata
from dweather_client.utils import listify_period, get_n_closest_station_ids
from dweather_client.ipfs_client import cat_station_df, cat_station_csv, cat_metadata
from dweather_client.df_utils import get_station_ids_with_icao
import ipfshttpclient
import datetime
import logging

HISTORICAL_START_YEAR = 1981

def cat_station_df_list(station_ids, pin=True, force_hash=None):
    batch_hash = force_hash
    if (force_hash is None):
        batch_hash = get_heads()['ghcnd']
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
                    client=client,
                    pin=pin,
                    force_hash=batch_hash
            ))
            except ipfshttpclient.exceptions.ErrorResponse:
                logging.warning("Station %s not found" % station_id)
                
    return station_content 

def cat_icao_stations(pin=True, force_hash=None):
    """
    For every station that has an icao code, load it into a dataframe and
    return them all as a list.
    """
    station_ids = get_station_ids_with_icao()
    return cat_station_df_list(station_ids, pin=pin, force_hash=force_hash)

def cat_n_closest_station_dfs(lat, lon, n, pin=True, force_hash=None):
    """
    Load the closest n stations to a given point into a list of dataframes.
    """
    if (force_hash is None):
        metadata = cat_metadata(get_heads()['ghcnd'])
    else:
        metadata = cat_metadata(force_hash)
    station_ids = get_n_closest_station_ids(lat, lon, metadata, n)
    return cat_station_df_list(station_ids, pin=pin, force_hash=force_hash)

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
