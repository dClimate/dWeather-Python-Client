"""
Use these functions to get historical climate data.
"""
from dweather_client.http_queries import get_station_csv, get_dataset_cell, get_metadata, get_heads
from dweather_client.aliases_and_units import \
    lookup_station_alias, STATION_UNITS_LOOKUP as SUL, METRIC_TO_IMPERIAL as M2I, IMPERIAL_TO_METRIC as I2M, FLASK_DATASETS, UNIT_ALIASES
from dweather_client.ipfs_errors import AliasNotFound, DataMalformedError
from dweather_client.grid_utils import snap_to_grid, conventional_lat_lon_to_cpc, cpc_lat_lon_to_conventional
from dweather_client.http_queries import flask_query
from dweather_client.struct_utils import tupleify
from dweather_client.df_loader import get_atcf_hurricane_df, get_historical_hurricane_df, get_simulated_hurricane_df
import datetime, pytz, csv
from astropy import units as u
import pandas as pd
import numpy as np
from timezonefinder import TimezoneFinder


def get_gridcell_history(
        lat,
        lon,
        dataset,
        snap_lat_lon_to_closest_valid_point=True,
        also_return_snapped_coordinates=False,
        protocol='https',
        also_return_metadata=False,
        use_imperial_units=True,
        return_result_as_counter=False):
    """
    Get the historical timeseries data for a gridded dataset in a dictionary,
    or, if return_result_as_counter is set to True, as a collections.Counter

    This is a dictionary of dates: climate values for a given dataset and 
    lat, lon.

    If snap_lat_lon_to_closest_valid_point is set to True (which it is
    by default), returns the history for the closest valid lat lon as
    determined by the dataset's metadata resolution.

    protocol is set to 'https' by default, but can also be set to
    'ipfs'. There are performance tradeoffs depending on which protocol is
    selected.

    return_result_as_counter is set to False by default, but if it is set
    to true, the historical timeseries will be returned as a collections.Counter
    instead of as a dict. collections.Counter is useful for performance sensitive
    aggregations, for example as in grid_utils.get_polygon_df

    also_return_metadata is set to False by default, but if set to True,
    returns the metadata next to the dict/counter within a tuple.

    use_imperial_units is set to True by default, but if set to False,
    will get the appropriate metric unit from aliases_and_units
    """
    metadata = get_metadata(get_heads()[dataset])

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

    history_dict = {}
    (lat, lon), resp_dict = flask_query(dataset, lat, lon)
    for k in resp_dict:
        if type(missing_value) == str:
            val = np.nan if resp_dict[k] == missing_value else float(resp_dict[k])
        else:
            val = np.nan if float(resp_dict[k]) == missing_value else float(resp_dict[k])
        datapoint = val * dweather_unit
        if converter is not None:
            datapoint = converter(datapoint)
        history_dict[k] = datapoint

    # try a timezone-based transformation on the times in case we're using an hourly set.
    try:
        tf = TimezoneFinder()
        local_tz = pytz.timezone(tf.timezone_at(lng=lon, lat=lat))
        tz_history_dict = {}
        for time in history_dict:
            tz_history_dict[pytz.utc.localize(time).astimezone(local_tz)] = history_dict[time]
        history_dict = tz_history_dict
    except AttributeError:  # datetime.date (daily sets) doesn't work with this, only datetime.datetime (hourly sets)
        pass

    result = history_dict
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
        max_lon=None):
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
        raise ValueError
    if ((min_lat is not None) or (min_lon is not None) or (max_lat is not None) or (max_lon is not None)) \
            and ((min_lat is None) or (min_lon is None) or (max_lat is None) or (max_lon is None)):
        raise ValueError
    if radius and min_lat:
        raise ValueError

    if source == "atcf":
        storm_getter = get_atcf_hurricane_df
    elif source == "historical":
        storm_getter = get_historical_hurricane_df
    elif source == "simulated":
        storm_getter = get_simulated_hurricane_df
    else:
        raise ValueError

    if radius:
        return storm_getter(basin, radius=radius, lat=lat, lon=lon)
    elif min_lat:
        return storm_getter(basin, min_lat=min_lat, min_lon=min_lon, max_lat=max_lat, max_lon=max_lon)
    else:
        return storm_getter(basin)

def get_station_history(
        station_id,
        weather_variable,
        dataset='ghcnd',
        protocol='https',
        use_imperial_units=True):
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
    csv_text = get_station_csv(station_id, station_dataset=dataset)
    column = lookup_station_alias(weather_variable)
    history = {}
    reader = csv.reader(csv_text.split('\n'))
    headers = next(reader)
    date_col = headers.index('DATE')
    data_col = headers.index(column)
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