"""
Helpful auxilliary functions that don't directly interact with IPFS.
Basically, it should go here if it doesn't need to import from df_loader or http_client,
"""
import math
import pandas as pd
import datetime

ICAO_LOOKUP_PATH = 'etc/airport-codes.csv'
CPC_LOOKUP_PATH = 'etc/cpc-grid-ids.csv'

def snap_to_grid(lat, lon, metadata):
    """ 
    Find the nearest (lat,lon) on IPFS for a given metadata file.
    return: lat, lon
    args:
        lat = -90 < lat < 90, float
        lon = -180 < lon < 180, float
        metadata: a dWeather metadata file

    """
    resolution = metadata['resolution']
    start = [metadata['latitude range'][0], metadata['longitude range'][0]] #start [lat, lon]
    end = [metadata['latitude range'][1], metadata['longitude range'][1]] #end [lat, lon]
    category = metadata['climate category']

    if 'cpc' in metadata['source data url']:
        start[0], start[1] = conventional_lat_lon_to_cpc(start[0], start[1])
        end[0], end[1] = conventional_lat_lon_to_cpc(end[0], end[1])
    # check that the lat lon is in the bounding box

    if category != 'rainfall':
        raise Exception('snap_to_grid() called on non rainfall dataset.')

    lat = round(math.floor(lat/resolution)*resolution + resolution/2, 3)
    lon = round(math.floor(lon/resolution)*resolution + resolution/2, 3)
    return lat, lon        


def cpc_lat_lon_to_conventional(lat, lon):
    """
    Convert a pair of coordinates from the idiosyncratic CPC lat lon
    format to the conventional lat lon format.
    """
    return lat, lon - 360


def conventional_lat_lon_to_cpc(lat, lon):
    """
    Convert a pair of coordinates from conventional (lat,lon)
    to the idiosyncratic CPC (lat,lon) format.
    """
    return lat, lon + 360


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


def listify_period(start_date, end_date):
    """
    Make a list of all dates from start_date to end_date, inclusive.
    Args:
        start_date (datetime.date): first date to include
        end_date (datetime.date): last date to include
    Returns:
        list of datetime.date objects
    """
    days_in_range = (end_date - start_date).days
    return [start_date + datetime.timedelta(n) for n in range(days_in_range + 1)]


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


def is_revision_final(dataset, revision_to_check, last_acceptable_revision):
    """
    See if a given dataset revision should be considered 'final'
    a final dataset is any dataset at or before the last acceptable one
    Args:
        dataset (str): the dataset name (e.g. prism_precip)
        revision_to_check (str): the revision tag under consideration for final status
        last_acceptable_revision (str): the last 'final' revision tag
    returns:
        bool true if the revision is considered final
    """
    # The dataset revision lists are ordered by accuracy so we simply compare the indicies in the list
    dataset_list = IPFSDatasets.datasets[dataset]
    return dataset_list.index(revision_to_check) <= dataset_list.index(last_acceptable_revision)

