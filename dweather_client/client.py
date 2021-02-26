"""
Use these functions to get historical climate data.
"""
from dweather_client.http_queries import get_station_csv
from dweather_client.aliases_and_units import STATION_COLUMN_LOOKUP as SCL, STATION_UNITS_LOOKUP as SUL
import csv, pint, datetime
import pandas as pd

def get_gridcell_history(
    lat, 
    lon, 
    dataset,
    snap_lat_lon_to_closest_valid_point=True,
    protocol='https', 
    return_result_as_dataframe=False,
    also_return_metadata=False, 
    use_imperial_units=True):
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
    pass


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
