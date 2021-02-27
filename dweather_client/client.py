"""
Use these functions to get historical climate data.
"""
from dweather_client.http_queries import get_station_csv, get_dataset_cell, get_metadata, get_heads
from dweather_client.aliases_and_units import \
    STATION_COLUMN_LOOKUP as SCL, STATION_UNITS_LOOKUP as SUL, METRIC_TO_IMPERIAL as MTI, IMPERIAL_TO_METRIC as ITM
from dweather_client.ipfs_errors import *
from dweather_client.grid_utils import snap_to_grid, conventional_lat_lon_to_cpc, cpc_lat_lon_to_conventional
import csv, pint, datetime
import pandas as pd


def get_gridcell_history(
    lat, 
    lon, 
    dataset,
    snap_lat_lon_to_closest_valid_point=True,
    also_return_snapped_coordinates=False,
    protocol='https', 
    return_result_as_dataframe=False,
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

    unit_reg = pint.UnitRegistry()
    unit_reg.default_format = ".%if" % int(metadata['filename decimal precision'])
    if use_imperial_units:
        try:
            unit_name = MTI[metadata['unit of measurement']]
        except KeyError:
            unit_name = metadata['unit of measurement']
    else:
        try:
            unit_name = ITM[metadata['unit of measurement']]
        except KeyError:
            unit_name = metadata['unit of measurement']

    if snap_lat_lon_to_closest_valid_point:
        lat, lon = snap_to_grid(lat, lon, metadata)

    if 'cpc' in metadata['source data url']:
        lat, lon = conventional_lat_lon_to_cpc(lat, lon)

    history_text = get_dataset_cell(lat, lon, dataset, metadata=metadata)
    day_strs = history_text.replace(',', ' ').split()

    dataset_start_date = datetime.datetime.strptime(metadata['date range'][0], "%Y/%m/%d").date()
    dataset_end_date = datetime.datetime.strptime(metadata['date range'][1], "%Y/%m/%d").date()
    timedelta = dataset_end_date - dataset_start_date
    days_in_record = timedelta.days + 1 # we have both the start and end date in the dataset so its the difference + 1

    if (len(day_strs) != days_in_record):
        raise DataMalformedError("Number of days in data file does not match the provided metadata")

    if 'temperature delimiter' in metadata:
        if return_result_as_counter:
            raise ValueError("Can't return temperature delimited record as counter")
        highs = {}
        lows = {}
        for i in range(days_in_record):
            date_iter = dataset_start_date + datetime.timedelta(days=i)
            if day_strs[i] == metadata["missing value"]:
                highs[date_iter], lows[date_iter] = 0, 0
            else:
                low, high = map(float, day_strs[i].split(metadata['temperature delimiter']))
                lows[date_iter] = unit_reg.Quantity(low, unit_name)
                highs[date_iter] = unit_reg.Quantity(high, unit_name)
                    
        history_dict = highs, lows

    else:
        history_dict = Counter({}) if return_result_as_counter else {}
        for i in range(days_in_record):
            date_iter = dataset_start_date + datetime.timedelta(days=i)
            if day_strs[i] == metadata["missing value"]:
                history_dict[date_iter] = 0 if return_result_as_counter else None
            else:
                history_dict[date_iter] = unit_reg.Quantity(float(day_strs[i]), unit_name)

    if 'cpc' in metadata['source data url']:
        lat, lon = cpc_lat_lon_to_conventional(lat, lon)

    result = history_dict
    if also_return_metadata:
        try:
            result = result + ({"metadata": metadata},)
        except TypeError:
            result = (result,) + ({"metadata": metadata},)
    if also_return_snapped_coordinates:
        try:
            result = result + ({"snapped to": (lat, lon)},)
        except TypeError:
            result = (result,) + ({"snapped to": (lat, lon)},)
    return result


def get_storm_history():
    pass

def get_station_history( \
    station_id, 
    columns,
    dataset='ghcnd', 
#    protocol='https', TODO
    return_result_as_dataframe=False,
#    also_return_metadata=False,  TODO
    use_imperial_units=True):
    """
    Takes in a station id and a column name or iterable of column names.

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
        '' or alias 'snow water equivalent' -- the water level in inches
        equivalent to the amount of snow currently on the ground at the
        time of the observation.

    Pass in a tuple of column names to get a list of dicts.

    The GHCN column names are fairly esoteric so a column_lookup
    dictionary will try to find a valid GHCN column name for common 
    aliases.

    """
    csv_text = get_station_csv(station_id, station_dataset=dataset)
    variables = ()
    for aliases in SCL:
        if columns in aliases:
            variables = variables + SCL[aliases] # assume "columns" is a single string
    if (len(variables) != 1):
        for aliases in SCL:
            for column in columns:
                if column in aliases:
                    variables = variables + SCL[aliases] # otherwise assume it's an iterable of strings
    dict_results = {}
    for variable in variables:
        reader = csv.reader(csv_text.split('\n'))
        column_names = next(reader)
        date_col = column_names.index('DATE')
        unit_reg = pint.UnitRegistry()
        unit_reg.default_format = SUL[variable]['precision']
        data_col = column_names.index(variable)
        data = {}
        for row in reader:
            try:
                if row[data_col] == '':
                    continue
            except IndexError:
                continue
            datapoint = unit_reg.Quantity( \
            	(float(row[data_col]) / 10.0 ), # data comes in a 10th of a mm or deg C.
                SUL[variable]['metric']
            )
            if use_imperial_units:
                datapoint = datapoint.to(SUL[variable]['imperial'])
            data[datetime.datetime.strptime(row[date_col], "%Y-%m-%d").date()] = datapoint
        dict_results[variable] = data
    
    if return_result_as_dataframe == False:
        # return only {date: observation} if a single column is passed in.
        return dict_results if len(dict_results) != 1 else dict_results[variables[0]]    
    else:
        final_df = None
        for variable in dict_results:
            intermediate_dict = {}
            intermediate_dict["DATE"] = [date for date in dict_results[variable]]
            intermediate_dict[variable] = [dict_results[variable][date] for date in dict_results[variable]]
            df = pd.DataFrame.from_dict(intermediate_dict)
            df.DATE = pd.to_datetime(df.DATE)
            df.index = df["DATE"]
            df.drop(df.columns[0], axis=1, inplace=True)
            try:
                final_df = final_df.merge(df, how="outer", on="DATE", sort=True)
            except AttributeError:
                final_df = df
        return final_df
