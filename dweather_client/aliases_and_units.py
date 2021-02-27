"""
Functions associated with aliases, units, converting between 
different conventions, finding grids and stations that are the closest to a 
given point.

In general, all the idiosyncratic reality-based things that one has to 
deal with.
"""
from dweather_client.ipfs_errors import AliasNotFound
import zeep, os
import pandas as pd

METRIC_TO_IMPERIAL = { \
    "mm": "inches",
    "millimeters": "inches",
    "millimeter": "inches",
    "degC": "degF",
    "degree_Celsius": "degF",
    "degrees Celsius": "degF"
}

IMPERIAL_TO_METRIC = { \
    "inches": "mm",
    "inch": "mm",
    "in": "mm",
    "degF": "degC",
    "degree_Fahrenheit": "degC",
    "degrees Fahrenheit": "degC"
}

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

STATION_UNITS_LOOKUP = { \
    'PRCP': {'imperial': "inches", 'metric': "millimeters", 'precision': '.3f'},
    'SNWD': {'imperial': "inches", 'metric': "millimeters", 'precision': '.3f'},
    'SNOW': {'imperial': "inches", 'metric': "millimeters", 'precision': '.3f'},
    'WESD': {'imperial': "inches", 'metric': "millimeters", 'precision': '.3f'},
    'TMAX': {'imperial': "degF", 'metric': "degC", 'precision': '.3f'},
    'TMIN': {'imperial': "degF", 'metric': "degC", 'precision': '.3f'},
}

parent_dir = os.path.dirname(os.path.abspath(__file__))
CPC_LOOKUP_PATH = os.path.join(parent_dir, '/etc/cpc-grid-ids.csv')
ICAO_LOOKUP_PATH = os.path.join(parent_dir, '/etc/airport-codes.csv')

def lookup_station_column_alias(alias):
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




