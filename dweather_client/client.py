"""
Use these functions to get historical climate data.
"""

def get_gridcell_dict(
	lat, 
	lon, 
	dataset,
	snap_lat_lon_to_closest_valid_point=True,
	protocol='https', 
    return_result_as_counter=False,
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

def get_gridcell_df(
	lat, 
	lon, 
	dataset,
	snap_lat_lon_to_closest_valid_point=True,
	protocol='https', 
	also_return_metadata=False, 
	use_imperial_units=False):

def get_storm_dict():

def get_storm_df():

def get_station_df( \
	station_id, 
	columns,
	dataset='ghcnd', 
	protocol='https',
	return_metadata=False,
	use_imperial_units=True):

def get_station_dict( \
	station_id, 
	columns,
	dataset='ghcnd', 
	protocol='https',
	also_return_metadata=False,
	use_imperial_units=True):
    """
    Takes in a station id and a column name or iterable of column names. 

    Gets the csv body associated with the station_id, defaulting to the
    ghcnd dataset. Pass in dataset='ghcnd-imputed-daily' for imputed,
    though note that ghcndi is temperature only as of this writing.

    Passing in use_imperial=False will return results in metric. Imperial
    is the default as Arbol is based in the USA and the bulk of our deals
    are done in imperial.

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
    variables = []
    for aliases in column_lookup:
        if columns in aliases:
            variables.append(column_lookup[aliases]) # assume "columns" is a single string
    if (len(variables) != 1):
        for aliases in column_lookup:
            for column in columns:
                if column in aliases:
                    variables.append(column_lookup[column]) # otherwise assume it's an iterable
    results = []
    reader = csv.reader(csv_text.split())
    column_names = next(reader)
    date_col = column_names.index('DATE')
    for variable in variables:
        data_col = column_names.index(column)
        data = {}
        for row in reader:
            if row[data_col] == '':
                continue
            # data comes in a 10th of a mm or deg C.
            datapoint = (float(row[data_col]) / 10.0 ) * units_lookup[variable]['metric']
            if use_imperial:
                datapoint = datapoint.to(units_lookup[variable]['imperial'])
            data[datetime.datetime.strptime(row[date_col], "%Y-%m-%d").date()] = datapoint
         results.append(data)
    return results if len(results) != 1 else results[0]



