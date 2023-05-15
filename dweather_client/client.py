"""
Use these functions to get historical climate data.
"""
from curses import meta
from astropy.units import equivalencies
from dweather_client.http_queries import get_metadata, get_heads, get_stations_metadata
from dweather_client.aliases_and_units import \
    get_to_units, lookup_station_alias, STATION_UNITS_LOOKUP as SUL, get_unit_converter, get_unit_converter_no_aliases, rounding_formula, rounding_formula_temperature, BOM_UNITS, UNIT_ALIASES
from dweather_client.struct_utils import tupleify, convert_nans_to_none
import datetime
import pytz
import csv
import json
import inspect
import numpy as np
import pandas as pd
from astropy import units as u
from timezonefinder import TimezoneFinder
from dweather_client import gridded_datasets
from dweather_client.storms_datasets import IbtracsDataset, AtcfDataset, SimulatedStormsDataset
from dweather_client.ipfs_queries import AustraliaBomStations, CedaBiomass, CmeStationsDataset, DutchStationsDataset, DwdStationsDataset, DwdHourlyStationsDataset, GlobalHourlyStationsDataset, JapanStations, StationDataset, EauFranceDataset,\
    YieldDatasets, FsaIrrigationDataset, AemoPowerDataset, AemoGasDataset, AesoPowerDataset, ForecastDataset, AfrDataset, DroughtMonitor, CwvStations, SpeedwellStations, TeleconnectionsDataset, CsvStationDataset, StationForecastDataset
from dweather_client.slice_utils import DateRangeRetriever, has_changed
from dweather_client.ipfs_errors import *
from io import StringIO
import ipfshttpclient

# Gets all gridded dataset classes from the datasets module
GRIDDED_DATASETS = {
    obj.dataset: obj for obj in vars(gridded_datasets).values()
    if inspect.isclass(obj) and type(obj.dataset) == str
}


def get_forecast_datasets():
    heads = get_heads()
    potential_sources = ['gfs', 'ecmwf']
    get_forecast_heads = []
    for head in heads:
        for source in potential_sources:
            if source in head:
                get_forecast_heads.append(head)
    return get_forecast_heads


def get_gridcell_history(
        lat,
        lon,
        dataset,
        also_return_snapped_coordinates=False,
        also_return_metadata=False,
        use_imperial_units=True,
        desired_units=None,
        convert_to_local_time=True,
        as_of=None,
        ipfs_timeout=None):
    """
    Get the historical timeseries data for a gridded dataset in a dictionary

    This is a dictionary of dates/datetimes: climate values for a given dataset and
    lat, lon.

    also_return_metadata is set to False by default, but if set to True,
    returns the metadata next to the dict within a tuple.

    desired_units will override use_imperial_units and attempt to convert the result into 
    the specified str unit

    use_imperial_units is set to True by default, but if set to False,
    will get the appropriate metric unit from aliases_and_units
    """
    try:
        metadata = get_metadata(get_heads()[dataset])
    except KeyError:
        raise DatasetError("No such dataset in dClimate")

    # set up units
    if not desired_units:
        converter, dweather_unit = get_unit_converter(
            metadata["unit of measurement"], use_imperial_units)
    else:
        converter, dweather_unit = get_unit_converter_no_aliases(
            metadata["unit of measurement"], desired_units)

    # get dataset-specific "no observation" value
    missing_value = metadata["missing value"]
    try:
        with GRIDDED_DATASETS[dataset](as_of=as_of, ipfs_timeout=ipfs_timeout) as dataset_obj:
            try:
                (lat, lon), str_resp_series = dataset_obj.get_data(lat, lon)
            except (ipfshttpclient.exceptions.ErrorResponse, ipfshttpclient.exceptions.TimeoutError, KeyError, FileNotFoundError) as e:
                raise CoordinateNotFoundError("Invalid coordinate for dataset")
    except KeyError:
        raise DatasetError("No such dataset in dClimate")

    # try a timezone-based transformation on the times in case we're using an hourly set.
    if convert_to_local_time:
        try:
            tf = TimezoneFinder()
            local_tz = pytz.timezone(tf.timezone_at(lng=lon, lat=lat))
            str_resp_series = str_resp_series.tz_localize(
                "UTC").tz_convert(local_tz)
        # datetime.date (daily sets) doesn't work with this, only datetime.datetime (hourly sets)
        except (AttributeError, TypeError):
            pass

    if type(missing_value) == str:
        resp_series = str_resp_series.replace(
            missing_value, np.NaN).astype(float)
    else:
        str_resp_series.loc[str_resp_series.astype(
            float) == missing_value] = np.NaN
        resp_series = str_resp_series.astype(float)

    resp_series = resp_series * dweather_unit
    if converter is not None:
        try:
            converted_resp_series = pd.Series(
                converter(resp_series.values), resp_series.index)
        except ValueError:
            raise UnitError("Specified unit is incompatible with original")
        if desired_units is not None:
            if converted_resp_series.values.unit.physical_type == "temperature":
                rounded_resp_array = np.vectorize(rounding_formula_temperature)(
                    str_resp_series, converted_resp_series)
            else:
                rounded_resp_array = np.vectorize(rounding_formula)(
                    str_resp_series, resp_series, converted_resp_series)
            final_resp_series = pd.Series(
                rounded_resp_array * converted_resp_series.values.unit, index=resp_series.index)
        else:
            final_resp_series = converted_resp_series
    else:
        final_resp_series = resp_series

    result = {k: convert_nans_to_none(
        v) for k, v in final_resp_series.to_dict().items()}

    if also_return_metadata:
        result = tupleify(result) + ({"metadata": metadata},)
    if also_return_snapped_coordinates:
        result = tupleify(result) + ({"snapped to": (lat, lon)},)
    return result


def get_forecast(
        lat,
        lon,
        forecast_date,
        dataset,
        also_return_snapped_coordinates=False,
        also_return_metadata=False,
        use_imperial_units=True,
        desired_units=None,
        convert_to_local_time=True,
        ipfs_timeout=None):

    if not isinstance(forecast_date, datetime.date):
        raise TypeError("Forecast date must be datetime.date")

    try:
        metadata = get_metadata(get_heads()[dataset])
    except KeyError:
        raise DatasetError("No such dataset in dClimate")

    # set up units
    # if not desired_units and not use_imperial_units:
    #     converter = None
    #     dweather_unit = u.Unit(metadata["unit of measurement"])
    # elif not desired_units:
    #     converter, dweather_unit = get_unit_converter(metadata["unit of measurement"], use_imperial_units)
    # else:
    #     converter, dweather_unit = get_unit_converter_no_aliases(metadata["unit of measurement"], desired_units)
    if not desired_units:
        converter, dweather_unit = get_unit_converter(
            metadata["unit of measurement"], use_imperial_units)
    else:
        converter, dweather_unit = get_unit_converter_no_aliases(
            metadata["unit of measurement"], desired_units)

    if 'gfs' in dataset:
        interval = 1
        con_to_cpc = True
    elif 'ecmwf' in dataset:
        interval = 3
        con_to_cpc = False

    try:
        with ForecastDataset(dataset, interval=interval, con_to_cpc=con_to_cpc, ipfs_timeout=ipfs_timeout) as dataset_obj:
            (lat, lon), str_resp_series = dataset_obj.get_data(
                lat, lon, forecast_date)
    except KeyError:
        raise DatasetError("No such dataset in dClimate")
    except (ipfshttpclient.exceptions.ErrorResponse, ipfshttpclient.exceptions.TimeoutError, KeyError, FileNotFoundError) as e:
        raise CoordinateNotFoundError("Invalid coordinate for dataset")

    if convert_to_local_time:
        try:
            tf = TimezoneFinder()
            local_tz = pytz.timezone(tf.timezone_at(lng=lon, lat=lat))
            str_resp_series = str_resp_series.tz_localize(
                "UTC").tz_convert(local_tz)
        # datetime.date (daily sets) doesn't work with this, only datetime.datetime (hourly sets)
        except (AttributeError, TypeError):
            pass

    missing_value = ""
    resp_series = str_resp_series.replace(missing_value, np.NaN).astype(float)
    resp_series = resp_series * dweather_unit

    if converter is not None:
        try:
            converted_resp_series = pd.Series(
                converter(resp_series.values), resp_series.index)
        except ValueError:
            raise UnitError("Specified unit is incompatible with original")
        if desired_units is not None:
            if converted_resp_series.values.unit.physical_type == "temperature":
                rounded_resp_array = np.vectorize(rounding_formula_temperature)(
                    str_resp_series, converted_resp_series)
            else:
                rounded_resp_array = np.vectorize(rounding_formula)(
                    str_resp_series, resp_series, converted_resp_series)
            final_resp_series = pd.Series(
                rounded_resp_array * converted_resp_series.values.unit, index=resp_series.index)
        else:
            final_resp_series = converted_resp_series
    else:
        final_resp_series = resp_series

    result = {"data": {k: convert_nans_to_none(
        v) for k, v in final_resp_series.to_dict().items()}}
    if also_return_metadata:
        result = {**result, "metadata": metadata}
    if also_return_snapped_coordinates:
        result = {**result, "snapped to": [lat, lon]}
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

    # Shift to context manager (cm) approach
    # Establish cm in first if statement, use it as a context manager in the second
    if source == "atcf":
        cm = AtcfDataset(ipfs_timeout=ipfs_timeout)
    elif source == "historical":
        cm = IbtracsDataset(ipfs_timeout=ipfs_timeout)
    elif source == "simulated":
        cm = SimulatedStormsDataset(ipfs_timeout=ipfs_timeout)
    else:
        raise ValueError("Invalid source")

    with cm as storm_getter:
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
        desired_units=None,
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
        'WSF5' -- max five second wind gust

    The GHCN column names are fairly esoteric so a column_lookup
    dictionary will try to find a valid GHCN column name for common 
    aliases.

    """
    if desired_units:
        to_unit = get_to_units(desired_units)
    try:
        with StationDataset(dataset, ipfs_timeout=ipfs_timeout) as dataset_obj:
            csv_text = dataset_obj.get_data(station_id)
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
        raise WeatherVariableNotFoundError(
            "Invalid weather variable for this station")
    for row in reader:
        try:
            if row[data_col] == '':
                continue
        except IndexError:
            continue
        datapoint = SUL[column]['vectorize'](float(row[data_col]))

        if desired_units:
            try:
                if to_unit.physical_type == "temperature":
                    converted = datapoint.to(
                        to_unit, equivalencies=u.temperature())
                    datapoint = rounding_formula_temperature(
                        row[data_col], converted.value) * to_unit
                else:
                    converted = datapoint.to(to_unit)
                    datapoint = rounding_formula(
                        row[data_col], datapoint.value, converted.value) * to_unit
            except ValueError:
                raise UnitError("Specified unit is incompatible with original")
        elif use_imperial_units:
            datapoint = SUL[column]['imperialize'](datapoint)

        history[datetime.datetime.strptime(
            row[date_col], "%Y-%m-%d").date()] = datapoint
    return history


def get_cme_station_history(station_id, weather_variable, use_imperial_units=True, desired_units=None, ipfs_timeout=None):
    try:
        # original cme set up
        with CmeStationsDataset(ipfs_timeout=ipfs_timeout) as dataset_obj:
            csv_text = dataset_obj.get_data(station_id)
    except KeyError:
        raise DatasetError("No such dataset in dClimate")
    except ipfshttpclient.exceptions.ErrorResponse:
        raise StationNotFoundError("Invalid station ID for dataset")
    metadata = get_metadata(get_heads()["cme_temperature_stations-daily"])
    unit = metadata["stations"][station_id]
    if desired_units:
        converter, dweather_unit = get_unit_converter_no_aliases(
            unit, desired_units)
    else:
        converter, dweather_unit = get_unit_converter(unit, use_imperial_units)
    history = {}
    reader = csv.reader(csv_text.split('\n'))
    headers = next(reader)
    date_col = headers.index('DATE')
    try:
        data_col = headers.index(weather_variable)
    except ValueError:
        raise WeatherVariableNotFoundError(
            "Invalid weather variable for this station")
    for row in reader:
        try:
            if row[data_col] == '':
                continue
        except IndexError:
            continue
        datapoint = float(row[data_col]) * dweather_unit
        if converter:
            try:
                converted = converter(datapoint)
            except ValueError:
                raise UnitError("Specified unit is incompatible with original")
            if desired_units:
                if dweather_unit.physical_type == "temperature":
                    final_datapoint = rounding_formula_temperature(
                        row[data_col], converted.value) * converted.unit
                else:
                    final_datapoint = rounding_formula(
                        row[data_col], datapoint.value, converted.value) * converted.unit
            else:
                final_datapoint = converted.round(2)
        else:
            final_datapoint = datapoint
        history[datetime.datetime.strptime(
            row[date_col], "%Y-%m-%d").date()] = final_datapoint
    return history


def get_hourly_station_history(dataset, station_id, weather_variable, use_imperial_units=True, desired_units=None, ipfs_timeout=None):

    # Get original units from metadata
    original_units = None
    metadata = get_metadata(get_heads()[dataset])
    station_metadata = metadata["station_metadata"][station_id]
    for climate_var in station_metadata:
        if climate_var['name'] == weather_variable:
            original_units = climate_var["unit"]
    if original_units == None:
        raise WeatherVariableNotFoundError(
            "Invalid weather variable for this station")
    try:
        if dataset == "dwd_hourly-hourly":
            with DwdHourlyStationsDataset(ipfs_timeout=ipfs_timeout) as dataset_obj:
                csv_text = dataset_obj.get_data(station_id, weather_variable)
        elif dataset == "ghisd-sub_hourly":
            with GlobalHourlyStationsDataset(ipfs_timeout=ipfs_timeout) as dataset_obj:
                csv_text = dataset_obj.get_data(station_id, weather_variable)
        else:
            raise DatasetError("No such dataset in dClimate")
    except ipfshttpclient.exceptions.ErrorResponse:
        raise StationNotFoundError("Invalid station ID for dataset")
    df = pd.read_csv(StringIO(csv_text))
    str_resp_series = df[weather_variable].astype(str)
    df = df.set_index("DATE")
    if "STATION" in df:
        del df["STATION"]
    if desired_units:
        converter, dweather_unit = get_unit_converter_no_aliases(
            original_units, desired_units)
    else:
        converter, dweather_unit = get_unit_converter(
            original_units, use_imperial_units)
    if converter:
        try:
            converted_resp_series = pd.Series(
                converter(df[weather_variable].values*dweather_unit), index=df.index)
        except ValueError:
            raise UnitError("Specified unit is incompatible with original")
        if desired_units is not None:
            rounded_resp_array = np.vectorize(rounding_formula_temperature)(
                str_resp_series, converted_resp_series)
            final_resp_series = pd.Series(
                rounded_resp_array * converted_resp_series.values.unit, index=df.index)
        else:
            final_resp_series = converted_resp_series
    else:
        final_resp_series = pd.Series(
            df[weather_variable].values*dweather_unit, index=df.index)
    result = {datetime.datetime.fromisoformat(k): convert_nans_to_none(
        v) for k, v in final_resp_series.to_dict().items()}
    return result


def get_csv_station_history(dataset, station_id, weather_variable, use_imperial_units=True, desired_units=None, ipfs_timeout=None):
    """
    This is almost an exact copy of get_hourly_station_history

    Over time, more and more stations will be fed through this function
    instead of the others here in client. That list currently stands at:

    -  inmet_brazil-hourly
    """
    # Get original units from metadata
    original_units = None
    metadata = get_metadata(get_heads()[dataset])
    # This is a list of possible variables with units
    # iterate through to see if the required variable is
    # available from the dataset queried
    data_dictionary = metadata["data dictionary"]
    for variable_key, variable_dict in data_dictionary.items():
        # This if will skip variables that aren't available to query
        # usually just dt
        if "api name" in variable_dict:
            if variable_dict["api name"] == weather_variable:
                original_key = variable_key
                original_units = variable_dict["unit of measurement"]
                column_name = variable_dict["column name"]
                # certain units don't convert properly eg mbar -> millibar
                # so we use UNIT_ALIASES to alias them
                try:
                    original_units = UNIT_ALIASES[original_units]
                except KeyError:
                    pass

    # if at this point we have no original units, the requested var
    # doesn't exist at all for this dataset
    if original_units == None:
        raise WeatherVariableNotFoundError(
            "Invalid weather variable for this dataset, none of the stations contain it")

    # Check each station to see if it has the same station name
    # if none do, then the station is invalid
    station_metadata = None
    stations_metadata = get_stations_metadata(
        get_heads()[dataset])["features"]
    for station in stations_metadata:
        if station["properties"]["file name"] == f"{station_id}.csv":
            station_metadata = station
    if station_metadata == None:
        raise StationNotFoundError("This is not a valid station name")

    # get full list of available variables from station metadata
    variable_keys = station_metadata["properties"]["variables"]

    # make sure requested variable is available for requested station
    # before continuing with retrieval
    if original_key not in variable_keys:
        raise WeatherVariableNotFoundError(
            "Invalid weather variable for this station")

    try:
        # RawSet style where we only want the most recent file
        if dataset in ["inmet_brazil-hourly"]:
            with CsvStationDataset(dataset=dataset, ipfs_timeout=ipfs_timeout) as dataset_obj:
                csv_text_list = [dataset_obj.get_data(
                    station_id, weather_variable)]
        # ClimateSet style where we need the entire linked list history
        elif dataset in ["ne_iso-hourly"]:
            with CsvStationDataset(dataset=dataset, ipfs_timeout=ipfs_timeout) as dataset_obj:
                csv_text_list = dataset_obj.get_data_recursive(
                    station_id, weather_variable)
        else:
            raise DatasetError("No such dataset in dClimate")
    except ipfshttpclient.exceptions.ErrorResponse:
        raise StationNotFoundError("Invalid station ID for dataset")

    # concat together all retrieved station csv texts
    dfs = []
    for csv_text in csv_text_list:
        dfs.append(pd.read_csv(StringIO(csv_text)))
    df = pd.concat(dfs, ignore_index=True)
    str_resp_series = df[column_name].astype(str)
    df = df.set_index("dt")
    if desired_units:
        converter, dweather_unit = get_unit_converter_no_aliases(
            original_units, desired_units)
    else:
        converter, dweather_unit = get_unit_converter(
            original_units, use_imperial_units)
    if converter:
        try:
            converted_resp_series = pd.Series(
                converter(df[column_name].values*dweather_unit), index=df.index)
        except ValueError:
            raise UnitError("Specified unit is incompatible with original")
        if desired_units is not None:
            rounded_resp_array = np.vectorize(rounding_formula_temperature)(
                str_resp_series, converted_resp_series)
            final_resp_series = pd.Series(
                rounded_resp_array * converted_resp_series.values.unit, index=df.index)
        else:
            final_resp_series = converted_resp_series
    else:
        final_resp_series = pd.Series(
            df[column_name].values*dweather_unit, index=df.index)
    result = {datetime.datetime.fromisoformat(k): convert_nans_to_none(
        v) for k, v in final_resp_series.to_dict().items()}
    return result


def get_station_forecast_history(dataset, station_id, forecast_date, desired_units=None, ipfs_timeout=None):
    try:
        with StationForecastDataset(dataset, ipfs_timeout=ipfs_timeout) as dataset_obj:
            csv_text = dataset_obj.get_data(station_id, forecast_date)
            history = {}
            reader = csv.reader(csv_text.split('\n'))
            headers = next(reader)
            date_col = headers.index('DATE')
            try:  # Make sure weather variable is correct.
                # at the moment the only variable is "SETT"
                data_col = headers.index("SETT")
            except ValueError:
                raise WeatherVariableNotFoundError(
                    "Invalid weather variable for this station")
            for row in reader:
                try:
                    if not row:
                        continue
                    history[datetime.datetime.strptime(
                        row[date_col], "%Y-%m-%d").date()] = float(row[data_col])
                except ValueError:
                    history[datetime.datetime.strptime(
                        row[date_col], "%Y-%m-%d").date()] = row[data_col]
            return history
    except ipfshttpclient.exceptions.ErrorResponse:
        raise StationNotFoundError("Invalid station ID for dataset")


def get_station_forecast_stations(dataset, forecast_date, desired_units=None, ipfs_timeout=None):
    with StationForecastDataset(dataset, ipfs_timeout=ipfs_timeout) as dataset_obj:
        csv_text = dataset_obj.get_stations(forecast_date)
        return json.loads(csv_text)


def get_european_station_history(dataset, station_id, weather_variable, use_imperial_units=True, desired_units=None, ipfs_timeout=None):
    try:
        if dataset == "dwd_stations-daily":
            cm = DwdStationsDataset(ipfs_timeout=ipfs_timeout)
        elif dataset == "dutch_stations-daily":
            cm = DutchStationsDataset(ipfs_timeout=ipfs_timeout)
        else:
            raise ValueError("invalid european dataset")

        with cm as dataset_obj:
            csv_text = dataset_obj.get_data(station_id)
    except KeyError:
        raise DatasetError("No such dataset in dClimate")
    except ipfshttpclient.exceptions.ErrorResponse:
        raise StationNotFoundError("Invalid station ID for dataset")
    metadata = get_metadata(get_heads()[dataset])

    station_metadata = metadata["station_metadata"][station_id]
    try:
        weather_var_index = [i for i in range(
            len(station_metadata)) if station_metadata[i]["name"] == weather_variable][0]
    except IndexError:
        raise WeatherVariableNotFoundError(
            "Invalid weather variable for this station")
    unit = station_metadata[weather_var_index]["unit"]
    multiplier = station_metadata[weather_var_index]["multiplier"]
    if desired_units:
        converter, dweather_unit = get_unit_converter_no_aliases(
            unit, desired_units)
    else:
        converter, dweather_unit = get_unit_converter(unit, use_imperial_units)
    history = {}
    reader = csv.reader(csv_text.split('\n'))
    headers = next(reader)
    date_col = headers.index('DATE')
    try:
        data_col = headers.index(weather_variable)
    except ValueError:
        raise WeatherVariableNotFoundError(
            "Invalid weather variable for this station")
    for row in reader:
        try:
            if row[data_col] == '':
                continue
        except IndexError:
            continue
        datapoint = (float(row[data_col]) * multiplier) * dweather_unit
        if converter:
            try:
                converted = converter(datapoint)
            except ValueError:
                raise UnitError("Specified unit is incompatible with original")
            if desired_units:
                if dweather_unit.physical_type == "temperature":
                    final_datapoint = rounding_formula_temperature(
                        row[data_col], converted.value) * converted.unit
                else:
                    final_datapoint = rounding_formula(
                        row[data_col], datapoint.value, converted.value) * converted.unit
            else:
                final_datapoint = converted.round(2)
        else:
            final_datapoint = datapoint
        history[datetime.datetime.strptime(
            row[date_col], "%Y-%m-%d").date()] = final_datapoint
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
    if dataset in ["rmasco_imputed-yearly", "rma_t_yield_imputed-single-value"] and commodity != "0081":
        raise ValueError(
            "Multipliers currently only available for soybeans (commodity code 0081)")
    try:
        with YieldDatasets(dataset, ipfs_timeout=ipfs_timeout) as dataset_obj:
            return dataset_obj.get_data(commodity, state, county)
    except ipfshttpclient.exceptions.ErrorResponse:
        raise ValueError("Invalid commodity/state/county code combination")


def get_irrigation_data(commodity, ipfs_timeout=None):
    """
    return:
        string containing irrigation data for commodity in csv format
    args:
        commodity (str), 4 digit code
    """
    try:
        with FsaIrrigationDataset(ipfs_timeout=ipfs_timeout) as dataset_obj:
            return dataset_obj.get_data(commodity)
    except ipfshttpclient.exceptions.ErrorResponse:
        raise ValueError("Invalid commodity code")


def get_japan_station_history(station_name, desired_units=None, as_of=None, ipfs_timeout=None):
    """
    return:
        dict with datetime keys and temperature Quantities as values
    """
    metadata = get_metadata(get_heads()["japan_meteo-daily"])
    with JapanStations(ipfs_timeout=ipfs_timeout, as_of=as_of) as dataset_obj:
        str_resp_series = dataset_obj.get_data(station_name)
    resp_series = str_resp_series.astype(float)
    if desired_units:
        unit = metadata["unit of measurement"]
        converter, dweather_unit = get_unit_converter_no_aliases(
            unit, desired_units)
        resp_series = resp_series * dweather_unit
        converted_resp_series = pd.Series(
            converter(resp_series.values), resp_series.index)
        rounded_resp_array = np.vectorize(rounding_formula_temperature)(
            str_resp_series, converted_resp_series)
        final_resp_series = pd.Series(
            rounded_resp_array * converted_resp_series.values.unit, index=resp_series.index)
        return final_resp_series.to_dict()
    else:
        return (resp_series * u.Unit("deg_C")).to_dict()


def get_cwv_station_history(station_name, as_of=None, ipfs_timeout=None):
    """
    return:
        dict with datetime keys and cwv Quantities as values
    """
    metadata = get_metadata(get_heads()["cwv-daily"])
    with CwvStations(ipfs_timeout=ipfs_timeout, as_of=as_of) as dataset_obj:
        str_resp_series = dataset_obj.get_data(station_name)
    resp_series = str_resp_series.astype(float)
    # CWV is a proprietary unscaled unit from the UK National Grid so use dimensionless unscaled
    return (resp_series * u.dimensionless_unscaled).to_dict()


def get_australia_station_history(station_name, weather_variable, desired_units=None, as_of=None, ipfs_timeout=None):
    """
    return:
        dict with datetime.date keys and weather variable Quantities (or strs in the case of GUSTDIR) as values
    """
    try:
        unit = BOM_UNITS[weather_variable]
    except KeyError:
        raise WeatherVariableNotFoundError(
            "Invalid weather variable for Australia station")
    with AustraliaBomStations(ipfs_timeout=ipfs_timeout, as_of=as_of) as dataset_obj:
        str_resp_series = dataset_obj.get_data(station_name)[weather_variable]
    if weather_variable == "GUSTDIR":
        return str_resp_series.replace("", np.nan).to_dict()
    resp_series = str_resp_series.replace("", np.nan).astype(float)
    if desired_units:
        converter, dweather_unit = get_unit_converter_no_aliases(
            unit, desired_units)
        resp_series = resp_series * dweather_unit
        converted_resp_series = pd.Series(
            converter(resp_series.values), resp_series.index)
        rounded_resp_array = np.vectorize(rounding_formula_temperature)(
            str_resp_series, converted_resp_series)
        final_resp_series = pd.Series(
            rounded_resp_array * converted_resp_series.values.unit, index=resp_series.index)
        return final_resp_series.to_dict()
    else:
        return (resp_series * u.Unit(unit)).to_dict()


def get_power_history(ipfs_timeout=None):
    """
    return:
        dict with datetime keys and values that are dicts with keys 'demand' and 'price'
    """
    with AemoPowerDataset(ipfs_timeout=ipfs_timeout) as dataset_obj:
        return dataset_obj.get_data()


def get_gas_history(ipfs_timeout=None):
    """
    return:
        dict with date keys and float values
    """
    with AemoGasDataset(ipfs_timeout=ipfs_timeout) as dataset_obj:
        return dataset_obj.get_data()


def get_alberta_power_history(ipfs_timeout=None):
    """
    return:
        dict with datetime keys and values that are dicts with keys 'price' 'ravg' and 'demand'
    """
    with AesoPowerDataset(ipfs_timeout=ipfs_timeout) as dataset_obj:
        return dataset_obj.get_data()


def get_drought_monitor_history(state, county, ipfs_timeout=None):
    try:
        with DroughtMonitor(ipfs_timeout=ipfs_timeout) as dataset_obj:
            return dataset_obj.get_data(state, county)
    except ipfshttpclient.exceptions.ErrorResponse:
        raise ValueError("Invalid state/county combo")


def get_ceda_biomass(year, lat, lon, unit, ipfs_timeout=None):
    """
    args:
        :year: (str) One of '2010', '2017', '2018', '2018-2010', 2018-2017'
        :lat: (float) Ranges from -40 to 80: latitude of northwest corner of desired square
        :lon: (float) Ranges from -180 to 180: longitude of northwest corner of desired square
        :unit: (str) 'AGB' (above-ground biomass) or 'AGB_SD' (above-ground biomass + standing dead)
    returns:
        BytesIO representing relevant GeoTiff File
    """
    try:
        with CedaBiomass(ipfs_timeout=ipfs_timeout) as dataset_obj:
            return dataset_obj.get_data(year, lat, lon, unit)
    except ipfshttpclient.exceptions.ErrorResponse:
        raise ValueError("Invalid paramaters with which to get biomass data")


def get_afr_history(ipfs_timeout=None):
    with AfrDataset(ipfs_timeout=ipfs_timeout) as dataset_obj:
        return dataset_obj.get_data()


def has_dataset_updated(dataset, slices, as_of, ipfs_timeout=None):
    """
    Determine whether any dataset updates generated after `as_of` affect any `slices` of date ranges.
    """
    with DateRangeRetriever(dataset, ipfs_timeout=ipfs_timeout) as dataset_obj:
        ranges = dataset_obj.get_data(as_of)
    return has_changed(slices, ranges)


def get_teleconnections_history(weather_variable, ipfs_timeout=None):
    with TeleconnectionsDataset(ipfs_timeout=ipfs_timeout) as dataset_obj:
        try:
            csv_text = dataset_obj.get_data(weather_variable)
        except ValueError:
            raise WeatherVariableNotFoundError(
                "Invalid weather variable for this station")
        history = {}
        reader = csv.reader(csv_text.split('\n'))
        headers = next(reader)
        date_col = headers.index('DATE')
        for row in reader:
            try:
                if row[data_col] == '':
                    continue
            except IndexError:  # Catch weird index issues that can occur
                continue
            # Values will either be a float value in string form (which need to be cast to a float), or an empty string
            try:
                history[datetime.datetime.strptime(
                    row[date_col], "%Y-%m-%d").date()] = float(row[data_col])
            except ValueError:
                history[datetime.datetime.strptime(
                    row[date_col], "%Y-%m-%d").date()] = row[data_col]
        return history


def get_eaufrance_history(station, weather_variable, use_imperial_units=False, desired_units=None, ipfs_timeout=None):
    try:
        with EauFranceDataset(ipfs_timeout=ipfs_timeout) as dataset_obj:
            csv_text = dataset_obj.get_data(station)
            df = pd.read_csv(StringIO(csv_text))
            df = df.set_index("DATE")
            original_units = "m^3/s"
            if desired_units:
                converter, dweather_unit = get_unit_converter_no_aliases(
                    original_units, desired_units)
            else:
                converter, dweather_unit = get_unit_converter(
                    original_units, use_imperial_units)
            if converter:
                try:
                    converted_resp_series = pd.Series(
                        converter(df[weather_variable].values*dweather_unit), index=df.index)
                except ValueError:
                    raise UnitError(
                        f"Specified unit is incompatible with original, original units are {original_units} and requested units are {desired_units}")
                if desired_units is not None:
                    rounded_resp_array = np.vectorize(rounding_formula_temperature)(
                        str_resp_series, converted_resp_series)
                    final_resp_series = pd.Series(
                        rounded_resp_array * converted_resp_series.values.unit, index=df.index)
                else:
                    final_resp_series = converted_resp_series
            else:
                final_resp_series = pd.Series(
                    df[weather_variable].values*dweather_unit, index=df.index)
            result = {datetime.date.fromisoformat(k): convert_nans_to_none(
                v) for k, v in final_resp_series.to_dict().items()}
        return result
    except ipfshttpclient.exceptions.ErrorResponse:
        raise StationNotFoundError("Invalid station ID for dataset")
