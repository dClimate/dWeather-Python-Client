"""
Use these functions to get historical climate data.
"""
from dweather_client.http_queries import get_metadata, get_heads
from dweather_client.aliases_and_units import \
    lookup_station_alias, STATION_UNITS_LOOKUP as SUL, METRIC_TO_IMPERIAL as M2I, IMPERIAL_TO_METRIC as I2M, UNIT_ALIASES
from dweather_client.struct_utils import tupleify, convert_nans_to_none
import datetime, pytz, csv, inspect
from astropy import units as u
import numpy as np
import pandas as pd
from timezonefinder import TimezoneFinder
from dweather_client import gridded_datasets
from dweather_client.storms_datasets import IbtracsDataset, AtcfDataset, SimulatedStormsDataset
from dweather_client.ipfs_queries import StationDataset, YieldDatasets, AemoPowerDataset, AemoGasDataset
from dweather_client.ipfs_errors import *
import ipfshttpclient

# Gets all gridded dataset classes from the datasets module
GRIDDED_DATASETS = {
    obj.dataset: obj for obj in vars(gridded_datasets).values()
    if inspect.isclass(obj) and type(obj.dataset) == str
}

def get_gridcell_history(
        lat,
        lon,
        dataset,
        also_return_snapped_coordinates=False,
        also_return_metadata=False,
        use_imperial_units=True,
        convert_to_local_time=True,
        ipfs_timeout=None):
    """
    Get the historical timeseries data for a gridded dataset in a dictionary

    This is a dictionary of dates/datetimes: climate values for a given dataset and
    lat, lon.

    also_return_metadata is set to False by default, but if set to True,
    returns the metadata next to the dict within a tuple.

    use_imperial_units is set to True by default, but if set to False,
    will get the appropriate metric unit from aliases_and_units
    """
    try:
        metadata = get_metadata(get_heads()[dataset])
    except KeyError:
        raise DatasetError("No such dataset in dClimate")

    # set up units
    str_u = metadata['unit of measurement']
    with u.imperial.enable():
        dweather_unit = UNIT_ALIASES[str_u] if str_u in UNIT_ALIASES else u.Unit(str_u)
    converter = None
    # if imperial is desired and dweather_unit is metric
    if use_imperial_units and (dweather_unit in M2I):
        converter = M2I[dweather_unit]
    # if metric is desired and dweather_unit is imperial
    elif (not use_imperial_units) and (dweather_unit in I2M):
        converter = I2M[dweather_unit]

    # get dataset-specific "no observation" value
    missing_value = metadata["missing value"]

    try:
        dataset_obj = GRIDDED_DATASETS[dataset](ipfs_timeout=ipfs_timeout)
    except KeyError:
        raise DatasetError("No such dataset in dClimate")

    try:
        (lat, lon), resp_series = dataset_obj.get_data(lat, lon)

    except (ipfshttpclient.exceptions.ErrorResponse, ipfshttpclient.exceptions.TimeoutError, KeyError, FileNotFoundError) as e:
        raise CoordinateNotFoundError("Invalid coordinate for dataset")

    # try a timezone-based transformation on the times in case we're using an hourly set.
    if convert_to_local_time:
        try:
            tf = TimezoneFinder()
            local_tz = pytz.timezone(tf.timezone_at(lng=lon, lat=lat))
            resp_series = resp_series.tz_localize("UTC").tz_convert(local_tz)
        except (AttributeError, TypeError):  # datetime.date (daily sets) doesn't work with this, only datetime.datetime (hourly sets)
            pass

    if type(missing_value) == str:
        resp_series = resp_series.replace(missing_value, np.NaN).astype(float)
    else:
        resp_series.loc[resp_series.astype(float) == missing_value] = np.NaN
        resp_series = resp_series.astype(float)
    
    resp_series = resp_series * dweather_unit
    if converter is not None:
        resp_series = pd.Series(converter(resp_series.values), resp_series.index)
    result = {k: convert_nans_to_none(v) for k, v in resp_series.to_dict().items()}
    
    if also_return_metadata:
        result = tupleify(result) + ({"metadata": metadata},)
    if also_return_snapped_coordinates:
        result = tupleify(result) + ({"snapped to": (lat, lon)},)
    return result

def get_tropical_storms(
        source,
        basin,
        radius=None,
        lat=None,
        lon=None,
        min_lat=None,
        min_lon=None,
        max_lat=None,
        max_lon=None,
        ipfs_timeout=None):
    """
    return:
        pd.DataFrame containing time series information on tropical storms
    args:
        source (str), one of: 'atcf', 'historical', 'simulated'
        basin (str),
            if source is 'atcf', one of: 'AL', 'CP', 'EP', 'SL'
            if source is 'simulated', one of: 'EP', 'NA', 'NI', 'SI', 'SP' or 'WP'
            if source is 'historical', one of: 'NI', 'SI', 'NA', 'EP', 'WP', 'SP', 'SA'
        radius (float), lat (float), lon (float),
            if given radius, lat, lon, will subset df to only include points within radius in km of the point (lat, lon)
        min_lat (float), min_lon (float), max_lat (float), max_lon (float)
            if given kwargs min_lat, min_lon, max_lat, max_lon, selects points within a bounding box.
    Note:
        (radius, lat, lon) and (min_lat, min_lon, max_lat, max_lon) are incompatible kwargs.
        i.e., if function is given args containing members from both tuples, raise ValueError
        in addition, the function's args must contain either all members of one of the above tuples, or none
        i.e., if function is given args containing some but not all of the members of one of the above tuples, raise ValueError
    """
    if ((radius is not None) or (lat is not None) or (lon is not None)) \
            and ((radius is None) or (lat is None) or (lon is None)):
        raise ValueError("Invalid args")
    if ((min_lat is not None) or (min_lon is not None) or (max_lat is not None) or (max_lon is not None)) \
            and ((min_lat is None) or (min_lon is None) or (max_lat is None) or (max_lon is None)):
        raise ValueError("Invalid args")
    if radius and min_lat:
        raise ValueError("Invalid args")

    if source == "atcf":
        storm_getter = AtcfDataset(ipfs_timeout=ipfs_timeout)
    elif source == "historical":
        storm_getter = IbtracsDataset(ipfs_timeout=ipfs_timeout)
    elif source == "simulated":
        storm_getter = SimulatedStormsDataset(ipfs_timeout=ipfs_timeout)
    else:
        raise ValueError("Invalid source")

    if radius:
        return storm_getter.get_data(basin, radius=radius, lat=lat, lon=lon)
    elif min_lat:
        return storm_getter.get_data(basin, min_lat=min_lat, min_lon=min_lon, max_lat=max_lat, max_lon=max_lon)
    else:
        return storm_getter.get_data(basin)
    
def get_station_history(
        station_id,
        weather_variable,
        use_imperial_units=True,
        dataset='ghcnd',     
        ipfs_timeout=None):
    """
    Takes in a station id and a weather variable.

    Gets the csv body associated with the station_id, defaulting to the
    ghcnd dataset. Pass in dataset='ghcnd-imputed-daily' for imputed,
    though note that ghcndi is only temperature as of this writing.

    Passing in use_imperial_units=False will return results in metric.
    Imperial is the default as Arbol is based in the USA and the bulk of our
    deals are done in imperial.

        'SNWD' or alias 'snow depth' -- the depth of snow at the time of the
        observation
        'SNOW' or alias 'snowfall -- the total snowfall observed since the
        last observation
        'WESD' or alias 'snow water equivalent', 'water equivalent snow depth' 
        -- the water level in inchesequivalent to the amount of snow currently 
        on the ground at the time of the observation.
        'TMAX' -- daily high temperature
        'TMIN' -- daily low temperature
        'PRCP' -- depth of rainfall

    The GHCN column names are fairly esoteric so a column_lookup
    dictionary will try to find a valid GHCN column name for common 
    aliases.

    """
    try:
        csv_text = StationDataset(dataset, ipfs_timeout=ipfs_timeout).get_data(station_id)
    except KeyError:
        raise DatasetError("No such dataset in dClimate")
    except ipfshttpclient.exceptions.ErrorResponse:
        raise StationNotFoundError("Invalid station ID for dataset")
    column = lookup_station_alias(weather_variable)
    history = {}
    reader = csv.reader(csv_text.split('\n'))
    headers = next(reader)
    date_col = headers.index('DATE')
    try:
        data_col = headers.index(column)
    except ValueError:
        raise WeatherVariableNotFoundError("Invalid weather variable for this station")
    for row in reader:
        try:
            if row[data_col] == '':
                continue
        except IndexError:
            continue
        datapoint = SUL[column]['vectorize'](float(row[data_col]))
        if use_imperial_units:
            datapoint = SUL[column]['imperialize'](datapoint)
        history[datetime.datetime.strptime(row[date_col], "%Y-%m-%d").date()] = datapoint

    return history

def get_yield_history(commodity, state, county, dataset="sco-yearly", ipfs_timeout=None):
    """
    return:
        string containing yield data in csv format
    args:
        commodity (str), 4 digit code
        state (str), 2 digit code
        county (str), 3 digit code
    Note:
        You can look up code values at:
        https://webapp.rma.usda.gov/apps/RIRS/AreaPlanHistoricalYields.aspx
    """
    if "imputed" in dataset and commodity != "0081":
        raise ValueError("Imputed currently only available for soybeans (commodity code 0081)")
    try:
        return YieldDatasets(dataset, ipfs_timeout=ipfs_timeout).get_data(commodity, state, county)
    except ipfshttpclient.exceptions.ErrorResponse:
        raise ValueError("Invalid commodity/state/county code combination")

def get_power_history(ipfs_timeout=None):
    """
    return:
        dict with datetime keys and values that are two member dicts with keys 'demand' and 'price'
    """
    return AemoPowerDataset(ipfs_timeout=ipfs_timeout).get_data()

def get_gas_history(ipfs_timeout=None):
    return AemoGasDataset(ipfs_timeout=ipfs_timeout).get_data()
