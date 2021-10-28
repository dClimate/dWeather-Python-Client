"""
Functions associated with aliases, units, converting between 
different conventions, finding grids and stations that are the closest to a 
given point.

In general, all the idiosyncratic reality-based things that one has to 
deal with.
"""
from dweather_client.ipfs_errors import AliasNotFoundError, UnitError
import zeep
import os
from astropy import units as u
from astropy.units import equivalencies, imperial
from math import floor, log10
import pandas as pd
import numpy as np

UNIT_ALIASES = {
    "kg/m**2": u.kg / u.m**2,
    "mm": u.mm,
    "degC": u.deg_C,
    "m s**-1": u.m / u.s,
    "degF": imperial.deg_F,
    "m of water equivalent": u.m,
    "vegetative health score": u.dimensionless_unscaled,
    "fraction": u.dimensionless_unscaled,
    "percentage": u.pct

}

METRIC_TO_IMPERIAL = {
    u.m: lambda q: q.to(imperial.inch),
    u.mm: lambda q: q.to(imperial.inch),
    u.deg_C: lambda q: q.to(imperial.deg_F, equivalencies=u.temperature()),
    u.K: lambda q: q.to(imperial.deg_F, equivalencies=u.temperature()),
    u.kg / u.m**2: lambda q: q.to(imperial.pound / imperial.ft ** 2),
    u.m / u.s: lambda q: q.to(imperial.mile / u.hour)
}

IMPERIAL_TO_METRIC = {
    imperial.inch: lambda q: q.to(u.mm),
    imperial.deg_F: lambda q: q.to(u.deg_C, equivalencies=u.temperature()),
    imperial.pound / imperial.ft ** 2: lambda q: q.to(u.kg / u.m**2),
    imperial.mile / u.hour: lambda q: q.to(u.m / u.s)
}

STATION_ALIASES_TO_COLUMNS = {
    ('SNWD',
     'snow depth',
     'snowdepth'): 'SNWD',
    ('SNOW',
     'snow fall',
     'snowfall',
     'snow'): 'SNOW',
    ('WESD',
     'snow water equivalent',
     'water equivalent snow depth'): 'WESD',
    ('TMAX',
     'highs',
     'max temperature',
     'temperature max',
     'maximum temperature',
     'temperature maximum',
     'max temp',
     'temp max',
     'maximum temp',
     'temp maximum'): 'TMAX',
    ('TMIN',
     'lows',
     'min temperature',
     'temperature min'
     'minimum temperature',
     'temperature minimum',
     'min temp',
     'temp min',
     'minimum temp',
     'temp minimum'): 'TMIN',
    ('PRCP',
     'precipitation',
     'precip',
     'rain',
     'rainfall'): 'PRCP'
}

def rounding_formula(str_val, original_val, converted_val, forced_precision=None):
    """
    Formula for determining how to round after a unit conversion. Can be vectorized to handle series/ndarrays
    Args:
        `str_val` (str) the original value as a string, with no rounding applied
        `original_val` (float) the original value converted to a float
        `converted_val` (float) the value after unit conversion has been applied
    Returns:
        converted value rounded to an appropriate number of decimals (float)
    """
    if converted_val == 0:
        return 0.0

    if forced_precision is not None:
        precision = forced_precision
    else:
        try:
            decimal = str_val.split('.')[1]
            precision = len(decimal)
        except IndexError:
            # No decimal
            precision = 0
    try:
        conversion_factor = converted_val / original_val
        exponent = -floor(log10(conversion_factor))
    except ValueError:
        # values are NaN
        return np.nan

    rounding_value = precision + exponent

    return round(converted_val, rounding_value)

def rounding_formula_temperature(str_val, converted_val, forced_precision=None):
    """
    Similar to `rounding_formula`, but use original precision instead of calculating
    """
    if forced_precision is not None:
        precision = forced_precision
    else:
        try:
            decimal = str_val.split('.')[1]
            precision = len(decimal)
        except IndexError:
            # No decimal
            precision = 0

    return round(converted_val, precision)

def get_unit_converter_no_aliases(original_units, desired_units):
    """
    Get an astropy Unit corresponding to `original_units` (str) and a converter (function) to convert to
    `desired_units` (str) Raises `UnitError` when unable to parse `desired_units` as Unit
    """
    degF = u.def_unit("degF", imperial.deg_F)
    degC = u.def_unit("degC", u.deg_C)
    with u.imperial.enable(), u.add_enabled_units([degF, degC]):
        dweather_unit = u.Unit(original_units)
        try:
            to_unit = u.Unit(desired_units)
        except ValueError:
            raise UnitError("Specified unit not recognized")
        if to_unit.physical_type == "temperature":
            converter = lambda q: q.to(to_unit, equivalencies=u.temperature())
        else:
            converter = lambda q: q.to(to_unit)
    return converter, dweather_unit

def get_to_units(desired_units):
    """
    Get the astropy Unit corresponding to `desired_units`. Raises `UnitError` when unable to parse
    `desired_units` as Unit
    """
    with u.imperial.enable():
        try:
            to_unit = u.Unit(desired_units)
        except ValueError:
            raise UnitError("Specified unit not recognized")
    return to_unit

def get_unit_converter(str_u, use_imperial_units):
    with u.imperial.enable():
        dweather_unit = UNIT_ALIASES[str_u] if str_u in UNIT_ALIASES else u.Unit(str_u)
    converter = None
    # if imperial is desired and dweather_unit is metric
    if use_imperial_units and (dweather_unit in METRIC_TO_IMPERIAL):
        converter = METRIC_TO_IMPERIAL[dweather_unit]
    # if metric is desired and dweather_unit is imperial
    elif (not use_imperial_units) and (dweather_unit in IMPERIAL_TO_METRIC):
        converter = IMPERIAL_TO_METRIC[dweather_unit]
    return converter, dweather_unit


def lookup_station_alias(alias):
    for aliases in STATION_ALIASES_TO_COLUMNS:
        if alias in aliases:
            return STATION_ALIASES_TO_COLUMNS[aliases]
    raise AliasNotFoundError("Invalid weather variable")

# see ghcnd readme ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt
STATION_UNITS_LOOKUP = {
    'PRCP': {'vectorize': lambda m: (m/10.0) * u.mm, 
             'imperialize': lambda m: m.to(imperial.inch)},
    'SNWD': {'vectorize': lambda m: m * u.mm, 
             'imperialize': lambda m: m.to(imperial.inch)},
    'SNOW': {'vectorize': lambda m: m * u.mm, 
             'imperialize': lambda m: m.to(imperial.inch)},
    'WESD': {'vectorize': lambda m: (m/10.0) * u.mm, 
             'imperialize': lambda m: m.to(imperial.inch)},
    'TMAX': {'vectorize': lambda m: (m/10.0) * u.deg_C, 
             'imperialize': lambda m: m.to(imperial.deg_F, equivalencies=u.temperature())},
    'TMIN': {'vectorize': lambda m: (m/10.0) * u.deg_C, 
             'imperialize': lambda m: m.to(imperial.deg_F, equivalencies=u.temperature())},
}

parent_dir = os.path.dirname(os.path.abspath(__file__))
CPC_LOOKUP_PATH = os.path.join(parent_dir, '/etc/cpc-grid-ids.csv')
ICAO_LOOKUP_PATH = os.path.join(parent_dir, '/etc/airport-codes.csv')


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
    icao_codes = pd.read_csv(os.path.join(ICAO_LOOKUP_PATH)).set_index('ICAO')  # get lookup table
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
    cpc_grids = pd.read_csv(os.path.join(CPC_LOOKUP_PATH)).set_index('Grid ID')  # dataframe of cpc grid to lat/lon lookup table
    myGrid = cpc_grids.iloc[grid_id]
    coords = [myGrid["Latitude"], myGrid["Longitude"]]
    coords = [coord+360 if coord < 0 else coord for coord in coords]  # converts negative coordinates to positive values

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
    result = client.service.getStationMetadata(
        '%s:%s:SNTL' % (str(snotel_id), str(state_fips)))
    ghcn_id = 'USS00%s' % result['actonId']
    return ghcn_id
