"""
Functions associated with aliases, units, converting between 
different conventions, finding grids and stations that are the closest to a 
given point.

In general, all the idiosyncratic "reality-based" things that one has to 
deal with when trying to get climate data stuff right.
"""

from ipfs_errors import AliasNotFound
import zeep, pint

STATION_COLUMN_LOOKUP = { \
    ('SNWD', 
        'snow depth', 
            'snowdepth'): ('SNWD',),
    ('SNOW', 
        'snow fall', 
            'snowfall', 
                'snow'): ('SNOW',),
    ('WESD', 
        'snow water equivalent',
            'water equivalent snow depth'): ('WESD',),
    ('TMAX', 
        'highs', 
            'max temperature', 
                'temperature max', 
                    'maximum temperature', 
                        'temperature maximum',
                            'max temp', 
                                'temp max', 
                                    'maximum temp', 
                                        'temp maximum'): ('TMAX',),
    ('TMIN', 
        'lows', 
            'min temperature', 
                'temperature min' 
                    'minimum temperature', 
                        'temperature minimum',
                            'min temp', 
                                'temp min', 
                                    'minimum temp', 
                                        'temp minimum'): ('TMIN',),
    ('temperature', 
        'temperatures', 
            'temp', 
                'temps'): ('TMAX', 'TMIN'),
    ('PRCP', 
        'precipitation', 
            'precip', 
                'rain', 
                    'rainfall'): ('PRCP',)
}

station_units = pint.UnitRegistry()
station_units.default_format = '.3f' # 3 units of precision
STATION_UNITS_LOOKUP = { \
    'PRCP': {'imperial': station_units.inch, 'metric': station_units.mm},
    'SNWD': {'imperial': station_units.inch, 'metric': station_units.mm},
    'SNOW': {'imperial': station_units.inch, 'metric': station_units.mm},
    'WESD': {'imperial': station_units.inch, 'metric': station_units.mm},
    'TMAX': {'imperial': station_units.degF, 'metric': station_units.degC},
    'TMIN': {'imperial': station_units.degF, 'metric': station_units.degC},
}

parent_dir = os.path.dirname(os.path.abspath(__file__))
CPC_LOOKUP_PATH = os.path.join(parent_dir, '/etc/cpc-grid-ids.csv')
ICAO_LOOKUP_PATH = os.path.join(parent_dir, '/etc/airport-codes.csv')


def lookup_station_column_alias(alias)
    """
    Get a valid GHCN station column for a given alias
    """
    for aliases in STATION_COLUMN_LOOKUP:
        if alias in aliases:
            return STATION_COLUMN_LOOKUP[aliases]
    raise AliasNotFound('The alias %s was not found in the station lookup' % alias)

def lookup_station_column_units(column_name):
    """
    Get the metric and imperial units for a given station column name
    """
    return STATION_UNITS_LOOKUP[lookup_station_column_alias(column_name)]

def icao_to_ghcn(icao_code):
    """ 
    Convert an icao airport code to ghcn.
    return:
        latitude, longitude
    args:
        icao = "xxxx" for example try "KLGA"
    """
    icao_codes = pd.read_csv(os.path.join(ICAO_LOOKUP_PATH)).set_index('ICAO') #get lookup table
    return icao_codes.loc[icao_code]["GHCN"]

def get_station_ids_with_icao():
    """
    Get a list of all the station id that are associated with stations that have an icao code.
    """
    icao_lookup = pd.read_csv(os.path.join(ICAO_LOOKUP_PATH)).set_index('ICAO')
    ids = []
    for index, row in icao_lookup.iterrows():
        if (row["GHCN"] not in ids):
            ids.append(row["GHCN"])
    return ids

def cpc_grid_to_lat_lon(grid_id):
    """ 
    Convert a cpc grid id to conventional lat lon via a lookup table.
    return:
        latitude, longitude
        
    args:
        grid_id = "1100" example
    
    """
    cpc_grids =  pd.read_csv(os.path.join(CPC_LOOKUP_PATH)).set_index('Grid ID') #dataframe of cpc grid to lat/lon lookup table
    myGrid = cpc_grids.iloc[grid_id]
    coords = [myGrid["Latitude"], myGrid["Longitude"]]
    coords = [coord+360 if coord < 0 else coord for coord in coords] #converts negative coordinates to positive values

    return coords[0], coords[1]

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

def lat_lon_to_rtma_grid(lat, lon, grid_history):
    grid_dict = build_rtma_reverse_lookup(grid_history)
    response = {}
    for timestamp in grid_dict:
        try:
            response[timestamp] = (grid_dict[timestamp]['lat'][lat], grid_dict[timestamp]['lon'][lon])
        except KeyError:
            response[timestamp] = (None, None)
            continue
    return response

def rtma_grid_to_lat_lon(x, y, grid_history):
    """
    x: the x coordinate of the rtma grid
    y: the y coordinate of the rtma grid
    grid_history: the raw string representation of the rtma grid history to be
        read directly from a file
    """
    grid_dict = build_rtma_lookup(grid_history)

    # get the lat/lon associated with x and y.
    return [(grid_dict[timestamp][0][y][x], grid_dict[timestamp][1][y][x]) for timestamp in grid_dict]

def snotel_to_ghcnd(snotel_id, state_fips):
    """
    Convert a SnoTEL ID to a station id that can be passed into GHCND or
    GHCNDi. snotel_id is a 3 or 4 digit integer, and the state_fips is 
    a two character abbrevation for the state -- for example CO.
    """
    client = zeep.Client('https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL')
    result = client.service.getStationMetadata( \
        '%s:%s:SNTL' % (str(snotel_id), str(state_fips)))
    ghcn_id = 'USS00%s' % result['actonId']
    return ghcn_id

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
    min_lat = metadata['latitude range'][0] #start [lat, lon]
    min_lon = metadata['longitude range'][0] #end [lat, lon]
    category = metadata['climate category']

    if 'cpc' in metadata['source data url']:
        min_lat, min_lon = conventional_lat_lon_to_cpc(min_lat, min_lon)

    # check that the lat lon is in the bounding box
    snap_lat = round(round((lat - min_lat)/resolution) * resolution + min_lat, 3)
    snap_lon = round(round((lon - min_lon)/resolution) * resolution + min_lon, 3)
    return snap_lat, snap_lon

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

def get_n_closest_station_ids(lat, lon, metadata, n):
    """
    Get the station ids for the <n> closest stations to a given lat lon. 
    Requires metadata of ghcnd to get station coordinates.
    """
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
