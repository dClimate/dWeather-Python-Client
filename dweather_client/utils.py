"""
Helpful auxilliary functions that don't directly interact with IPFS.
This module specifically excludes pandas
"""
import math
import datetime
from heapq import heappush, heappushpop
from geopy.distance import geodesic

def rtma_grid_to_lat_lon(x, y, grid_history):
    """
    x: the x coordinate of the rtma grid
    y: the y coordinate of the rtma grid
    grid_history: the raw string representation of the rtma grid history to be
        read directly from a file
    """

    # turn the string representation of the grid history into a data structure.
    # {timestamp: (lat_list, lon_list), timestamp: (lat_list, lon_list)}
    grid_history = grid_history.split('\n\n')
    grid_dict = {grid_history[0]: [grid_history[1], grid_history[2]], grid_history[3]: [grid_history[4], grid_history[5]]}
    for timestamp in grid_dict:
        for dimension in (0, 1):
            grid_dict[timestamp][dimension] = [y.strip('][').split(', ') for y in grid_dict[timestamp][dimension].split('\n')]

    # get the lat/lon associated with x and y.
    return [(grid_dict[timestamp][0][y][x], grid_dict[timestamp][1][y][x]) for timestamp in grid_dict]

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
    min_lat = metadata['latitude range'][0] #start [lat, lon]
    min_lon = metadata['longitude range'][0] #end [lat, lon]
    category = metadata['climate category']

    if 'cpc' in metadata['source data url']:
        min_lat, min_lon = conventional_lat_lon_to_cpc(min_lat, min_lon)

    # check that the lat lon is in the bounding box
    snap_lat = round(round((lat - min_lat)/resolution) * resolution + min_lat, 3)
    snap_lon = round(round((lon - min_lon)/resolution) * resolution + min_lon, 3)
    return snap_lat, snap_lon

def get_n_closest_station_ids(lat, lon, metadata, n):
    pq = []
    for feature in metadata["stations"]["features"]:
        s_lat = float(feature["geometry"]["coordinates"][0])
        s_lon = float(feature["geometry"]["coordinates"][1])
        distance = geodesic([lat, lon], [s_lat, s_lon]).miles
        station_id = feature["properties"]["station id"]
        if (len(pq) >= n):
            heappushpop(pq, (1 / distance, station_id))
        else:
            heappush(pq, (1 / distance, station_id))
    return [pair[1] for pair in pq]

def cpc_lat_lon_to_conventional(lat, lon):
    """
    Convert a pair of coordinates from the idiosyncratic CPC lat lon
    format to the conventional lat lon format.
    """
    if (lon >= 180):
        return lat, lon - 360
    else:
        return lat, lon


def conventional_lat_lon_to_cpc(lat, lon):
    """
    Convert a pair of coordinates from conventional (lat,lon)
    to the idiosyncratic CPC (lat,lon) format.
    """
    if (lon < 0):
        return lat, lon + 360
    else:
        return lat, lon


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

def celcius_to_fahrenheit(deg_c):
    return round((deg_c * 9/5) + 32, 5)

