"""
Various utilities associated with the coordinate grid system.
"""
from dweather_client.http_queries import get_heads, get_metadata
try:
    import geopandas as gpd
except:
    gpd = None
from geopy.distance import geodesic
from heapq import heappush, heappushpop
import pandas as pd
import numpy as np
import datetime, math


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
        bbox_size = int( \
            abs(bounding_box[1].y - bounding_box[0].y) *
            abs(bounding_box[1].x - bounding_box[0].x) /
            metadata['resolution'] /
            metadata['resolution']
        )
        logging.info("Building %s (%i of %i polygons)" % (row['state'], index + 1, len(polygons.index)))
        logging.info("Matching points within a bounding box of %i points." % bbox_size)
        exec_counter = 0
        poly_counter = Counter({})
        lat_bounds = [lat for lat in sorted((bounding_box[0].y, bounding_box[1].y))] # gotta sort for np.arange
        lon_bounds = [lon for lon in sorted((bounding_box[0].x, bounding_box[1].x))]
        for latitr in np.arange(lat_bounds[0], lat_bounds[1], metadata['resolution']):
            for lonitr in np.arange(lon_bounds[0], lon_bounds[1], metadata['resolution']):
                if ((exec_counter % 10000) == 0):
                    logging.info("Scanning point %i of %i" % (exec_counter, bbox_size))
                if Point(lonitr, latitr).within(row['geometry']):
                    slat, slon = snap_to_grid(latitr, lonitr, metadata)
                    logging.info("Found match at (%s, %s), point %i of %i" % ( \
                        "{:.3f}".format(slat),
                        "{:.3f}".format(slon),
                        exec_counter,
                        bbox_size
                    ))
                    try:
                        if 'cpc' in dataset:
                            slat, slon = conventional_lat_lon_to_cpc(slat, slon)
                        rain_counter = get_rainfall_dict(slat, slon, dataset, get_counter=True)
                        poly_counter = poly_counter + rain_counter
                    except:
                        logging.warning("Could not retrieve data for (%s, %s)" % ("{:.3f}".format(slat), "{:.3f}".format(slon)))
                        exec_counter = exec_counter + 1
                        continue
                exec_counter = exec_counter + 1
        for day in poly_counter:
            df.at[day.strftime('%Y-%m-%d'), row['state']] += poly_counter[day]
    return df 

def haversine_vectorize(lon1, lat1, lon2, lat2):
    """
    Vectorized version of haversine great circle calculation. 
    return: 
        distance in km between `(lat1, lon1)` and `(lat2, lon2)`

    args:
        lon1 (float) first longitude coord
        lat1 (float) first latitude coord
        lon2 (float) second longitude coord
        lat2 (float) second latitude coord
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    newlon = lon2 - lon1
    newlat = lat2 - lat1
    haver_formula = np.sin(newlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(newlon/2.0)**2
    dist = 2 * np.arcsin(np.sqrt(haver_formula))
    km = 6367 * dist
    return km


def nearby_storms(df, c_lat, c_lon, radius): 
    """
    return:
        DataFrame with all time series points in `df` within `radius` in km of the point `(c_lat, c_lon)`

    args:
        c_lat (float): latitude coordinate of bounding circle
        c_lon (float): longitude coordinate of bounding circle
    """ 
    dist = haversine_vectorize(df['lon'], df['lat'], c_lon, c_lat)
    return df[dist < radius]

def get_n_closest_station_ids(lat, lon, n, metadata=None):
    """
    Get the station ids for the <n> closest stations to a given lat lon. 
    Requires metadata of ghcnd to get station coordinates.
    """
    if metadata == None:
        metadata = get_metadata(get_heads()['ghcnd'])
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

def build_rtma_lookup(grid_history):
    """
    turn the string representation of the grid history into a data structure.
    {timestamp: (lat_list, lon_list), timestamp: (lat_list, lon_list)}
    """
    grid_history = grid_history.split('\n\n')
    grid_dict = {grid_history[0]: [grid_history[1], grid_history[2]], grid_history[3]: [grid_history[4], grid_history[5]]}
    for timestamp in grid_dict:
        for dimension in (0, 1):
            grid_dict[timestamp][dimension] = [y.strip('][').split(', ') for y in grid_dict[timestamp][dimension].split('\n')]
    return grid_dict

def build_rtma_reverse_lookup(grid_history):
    """
    Reverse index the rtma lookup data structure to enable querying by lat lon.
    {timestamp: {'lat': {latitude: (x, y)}}, {'lon': {longitude: (x, y)}}}
    Example query: 
        rev_grid_dict['2011-01-01T00:00:00']['lat']['20.191999000000006']
        rev_grid_dict['2011-01-01T00:00:00']['lon']['238.445999']
    """
    grid_dict = build_rtma_lookup(grid_history)
    rev_grid_dict = {}
    for timestamp in grid_dict:
        rev_grid_dict[timestamp] = {'lat': {}, 'lon': {}}
        # reindex lat
        for y in range(0, len(grid_dict[timestamp][0])):
            for x in range(0, len(grid_dict[timestamp][0][y])):
                if grid_dict[timestamp][0][y][x] in rev_grid_dict[timestamp]['lat']:
                    logging.error("Conflict in parsing rtma grid history.")
                rev_grid_dict[timestamp]['lat'][grid_dict[timestamp][0][y][x]] = (x, y)
        # reindex lon
        for y in range(0, len(grid_dict[timestamp][1])):
            for x in range(0, len(grid_dict[timestamp][1][y])):
                if grid_dict[timestamp][1][y][x] in rev_grid_dict[timestamp]['lon']:
                    logging.error('Conflict in Parsing rtma grid history.')
                rev_grid_dict[timestamp]['lon'][grid_dict[timestamp][1][y][x]] = (x, y)
    return rev_grid_dict

def zero_three_sixty_to_negative_one_eighty_one_eighty(lat, lon):
    return cpc_lat_lon_to_conventional(lat, lon)

def negative_one_eighty_one_eighty_to_zero_three_sixty(lat, lon):
    return conventional_lat_lon_to_cpc(lat, lon)

def cpc_lat_lon_to_conventional(lat, lon):
    """
    Convert a pair of coordinates from the idiosyncratic CPC lat lon
    format to the conventional lat lon format.
    """
    lat, lon = float(lat), float(lon)
    if (lon >= 180):
        return lat, lon - 360
    else:
        return lat, lon


def conventional_lat_lon_to_cpc(lat, lon):
    """
    Convert a pair of coordinates from conventional (lat,lon)
    to the idiosyncratic CPC (lat,lon) format.
    """
    lat, lon = float(lat), float(lon)
    if (lon < 0):
        return lat, lon + 360
    else:
        return lat, lon


def snap_to_grid(lat, lon, metadata):
    """ 
    Find the nearest valid (lat,lon) for a given metadata file and arbitrary
    "unsnapped" lat lon.

    Use this when you want to query a gridded dataset for some arbitrary
    point.

    return: lat, lon
    args:
        lat = -90 < lat < 90, float
        lon = -180 < lon < 180, float
        metadata: a dWeather metadata file

    """
    resolution = metadata['resolution']
    min_lat = metadata['latitude range'][0]
    min_lon = metadata['longitude range'][0]
    precision = metadata['filename decimal precision']

    if 'source data url' in metadata and 'cpc' in metadata['source data url']:
        min_lat, min_lon = cpc_lat_lon_to_conventional(min_lat, min_lon)

    snap_lat = round(round((lat - min_lat)/resolution) * resolution + min_lat, precision)
    snap_lon = round(round((lon - min_lon)/resolution) * resolution + min_lon, precision)

    return snap_lat, snap_lon
