"""
Helpful auxilliary functions that don't directly interact with IPFS.
Basically, it should go here if it doesn't need to import from df_loader or http_client,
this module uses pandas
"""
import math
import pandas as pd
import datetime
import os

CPC_LOOKUP_PATH = 'dweather_client/etc/cpc-grid-ids.csv'
ICAO_LOOKUP_PATH = 'dweather_client/etc/airport-codes.csv'


def dataframeify(dict_or_df):
    """
    Convert a dict to a dataframe.
    If "dict" is already a dataframe, just return it.
    """
    if isinstance(dict_or_df, pd.DataFrame):
        return dict_or_df
    else:
        return pd.DataFrame.from_dict(dict_or_df, orient="index")


def cpc_grid_to_lat_lon(grid):
    """ 
    Convert a cpc grid id to conventional lat lon via a lookup table.
    return:
        latitude, longitude
        
    args:
        grid = "1100" example
    
    """
    cpc_grids =  pd.read_csv(os.path.join(CPC_LOOKUP_PATH)).set_index('Grid ID') #dataframe of cpc grid to lat/lon lookup table
    myGrid = cpc_grids.iloc[grid]
    coords = [myGrid["Latitude"], myGrid["Longitude"]]
    coords = [coord+360 if coord < 0 else coord for coord in coords] #converts negative coordinates to positive values
    
    return coords[0], coords[1]


def icao_to_ghcn(icao):
    """ 
    Convert an icao airport code to ghcn.
    return:
        latitude, longitude
    args:
        icao = "xxxx" for example try "KLGA"
    """
    icao_codes = pd.read_csv(os.path.join(ICAO_LOOKUP_PATH)).set_index('ICAO') #get lookup table
    return icao_codes.loc[icao]["GHCN"]

def period_slice_df(df, ps, pe, yrs):
    """ 
    Slice a dataframe by period where n is based on years lookback 
    
    return:
        pd.DataFrame()
    
    args:
        df = any pd.DataFrame() with a datetime.date() index
        ps = datetime.date object (contract period start)
        ps = datetime.date object (contract period end)
        yrs = 30 (years lookback)
        
    limitations:
        max period is 364 days
        
    """

    mydf = pd.DataFrame()
    
    leftDay = ~((df.index.day < ps.day) & (df.index.month == ps.month))
    rightDay = ~((df.index.day > pe.day) & (df.index.month == pe.month))
    #first year just get end
    if ps.year < pe.year:
        firstYear = ps.year - yrs
        yr = df.index.year == firstYear
        mon = df.index.month >= ps.month
        #a boolean mask to cut the left and right half month days if need be
        mydf = mydf.append(df[(yr)&(mon)&(leftDay)])
        #everything in betweeng
        for yrss in range(ps.year - (yrs - 1), pe.year - 1):
            ##boolean mask for now, better method needed##
            yearChunk = (df.index.year == yrss)&((df.index.month<=pe.month)|(df.index.month>=ps.month))
            yearChunk = (yearChunk)&leftDay&rightDay
            mydf = mydf.append(df[yearChunk])
        #build last year
        mydf = mydf.append(df[rightDay&(df.index.month<=pe.month)&(df.index.year==pe.year-1)])
    else:
        mydf = df[leftDay&rightDay&(df.index.month>=ps.month)&(df.index.month<=pe.month)&(df.index.year>=ps.year-yrs)]
            
    return mydf

