"""
Use these functions to get historical climate data.
"""
from dweather_client.http_queries import get_station_csv, get_dataset_cell, get_metadata, get_heads
from dweather_client.aliases_and_units import \
    STATION_COLUMN_LOOKUP as SCL, STATION_UNITS_LOOKUP as SUL, METRIC_TO_IMPERIAL as MTI, IMPERIAL_TO_METRIC as ITM, FLASK_DATASETS, UNIT_ALIASES, DATASET_ALIASES
from dweather_client.ipfs_errors import AliasNotFound, DataMalformedError
from dweather_client.grid_utils import snap_to_grid, conventional_lat_lon_to_cpc, cpc_lat_lon_to_conventional
from dweather_client.http_queries import flask_query, get_prismc_dict
import datetime
import pytz
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
        # return_result_as_dataframe=False,
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
    dataset_name = DATASET_ALIASES[dataset] if dataset in DATASET_ALIASES else dataset

    metadata = get_metadata(get_heads()[dataset_name])
    unit_str = metadata["unit of measurement"]

    try:
        dataset_units = UNIT_ALIASES[unit_str]
    except KeyError:
        raise AliasNotFound("Invalid unit in metadata")
    if use_imperial_units:
        output_units = MTI[dataset_units] if dataset_units in MTI else dataset_units
    else:
        output_units = ITM[dataset_units] if dataset_units in ITM else dataset_units

    if dataset in FLASK_DATASETS:
        missing_value = metadata["missing value"]
        history_dict = {}
        (lat, lon), resp_dict = flask_query(dataset, lat, lon)
        if dataset == "rtma_pcp-hourly":
            tf = TimezoneFinder()
            local_tz = pytz.timezone(tf.timezone_at(lng=lon, lat=lat))
        for k in resp_dict:
            val = np.nan if resp_dict[k] == missing_value else float(resp_dict[k])
            datapoint = val * dataset_units
            if output_units != dataset_units:
                datapoint = datapoint.to(output_units)
            if dataset == "rtma_pcp-hourly":
                k = pytz.utc.localize(k).astimezone(local_tz)
            history_dict[k] = datapoint

    elif "prism" in dataset:
        lat, lon = snap_to_grid(lat, lon, metadata)
        missing_value = metadata["missing value"]
        if dataset == "prism-precip":
            history_dict = {}
            resp_dict = get_prismc_dict(lat, lon, "precip")
            for k in resp_dict:
                val = np.nan if resp_dict[k] == missing_value else resp_dict[k]
                datapoint = val * dataset_units
                if output_units != dataset_units:
                    datapoint = datapoint.to(output_units)
                history_dict[k] = datapoint

        elif dataset == "prism-temp":
            res_tmin = get_prismc_dict(lat, lon, "tmin")
            res_tmax = get_prismc_dict(lat, lon, "tmax")
            history_dicts = [res_tmin, res_tmax]
            for res in history_dicts:
                for k in res:
                    val = np.nan if res[k] == missing_value else res[k]
                    datapoint = val * dataset_units
                    if output_units != dataset_units:
                        datapoint = datapoint.to(output_units, equivalencies=u.temperature())
                    res[k] = datapoint
            history_dict = tuple(history_dicts)
    else:
        if snap_lat_lon_to_closest_valid_point:
            lat, lon = snap_to_grid(lat, lon, metadata)

        if 'source data url' in metadata and 'cpc' in metadata['source data url']:
            lat, lon = conventional_lat_lon_to_cpc(lat, lon)

        history_text = get_dataset_cell(lat, lon, dataset, metadata=metadata)
        day_strs = history_text.replace(',', ' ').split()

        dataset_start_date = datetime.datetime.strptime(metadata['date range'][0], "%Y/%m/%d").date()
        dataset_end_date = datetime.datetime.strptime(metadata['date range'][1], "%Y/%m/%d").date()
        timedelta = dataset_end_date - dataset_start_date
        days_in_record = timedelta.days + 1  # we have both the start and end date in the dataset so its the difference + 1

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
                    low_datapoint, high_datapoint = np.nan * dataset_units, np.nan * dataset_units
                else:
                    low, high = map(float, day_strs[i].split(metadata['temperature delimiter']))
                    low_datapoint, high_datapoint = low * dataset_units, high * dataset_units
                if output_units != dataset_units:
                    low_datapoint = low_datapoint.to(output_units, equivalencies=u.temperature())
                    high_datapoint = high_datapoint.to(output_units, equivalencies=u.temperature())
                highs[date_iter] = high_datapoint
                lows[date_iter] = low_datapoint

            history_dict = highs, lows

        else:
            history_dict = Counter({}) if return_result_as_counter else {}
            for i in range(days_in_record):
                date_iter = dataset_start_date + datetime.timedelta(days=i)
                if day_strs[i] == metadata["missing value"]:
                    datapoint = np.nan * dataset_units
                else:
                    datapoint = float(day_strs[i]) * dataset_units
                if output_units != dataset_units:
                    datapoint = datapoint.to(output_units)
                history_dict[date_iter] = datapoint

        if 'source data url' in metadata and 'cpc' in metadata['source data url']:
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


def get_station_history(
        station_id,
        column,
        dataset='ghcnd',
        protocol='https',
        return_result_as_dataframe=False,
        #    also_return_metadata=False,  TODO
        use_imperial_units=True):
    """
    Takes in a station id and a column name.

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

    The GHCN column names are fairly esoteric so a column_lookup
    dictionary will try to find a valid GHCN column name for common 
    aliases.

    """
    csv_text = get_station_csv(station_id, station_dataset=dataset)
    variables = ()
    for aliases in SCL:
        if columns in aliases:
            variable = SCL[aliases]
    dict_results = {}
    reader = csv.reader(csv_text.split('\n'))
    column_names = next(reader)
    date_col = column_names.index('DATE')
    data_col = column_names.index(variable)
    data = {}
    for row in reader:
        try:
            if row[data_col] == '':
                continue
        except IndexError:
            continue
        datapoint = SUL[variable]['vectorize'](float(row[data_col]))
        if use_imperial_units:
            datapoint = datapoint.to(SUL[variable]['imperial'])
        data[datetime.datetime.strptime(row[date_col], "%Y-%m-%d").date()] = datapoint
    dict_results[variable] = data

    if return_result_as_dataframe == False:
        return dict_results
    else:
        intermediate_dict["DATE"] = [date for date in dict_results[variable]]
        intermediate_dict[variable] = [dict_results[variable][date] for date in dict_results[variable]]
        df = pd.DataFrame.from_dict(intermediate_dict)
        df.DATE = pd.to_datetime(df.DATE)
        df.index = df["DATE"]
        df.drop(df.columns[0], axis=1, inplace=True)
        return final_df

# def get_station_history_experimen( \
#     station_id,
#     column,
#     dataset='ghcnd',
#     protocol='https',
#     return_result_as_dataframe=False,
# #    also_return_metadata=False,  TODO
#     use_imperial_units=True):
