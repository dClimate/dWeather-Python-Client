"""
Some functions for organizing dWeather query results into dataframe formats relevant to Arbol's use cases.
Mainly related to summing and averaging over time.
This module imports pandas and numpy, the arithmetic.py module should not
"""
import numpy as np
import pandas as pd
from dweather_client.http_client import get_rainfall_dict, get_rev_rainfall_dict
from dweather_client.utils import listify_period
import datetime

HISTORICAL_START_YEAR = 1981

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