"""
Basic functions for getting data from a dWeather gateway via https.
"""
import requests, datetime, io, gzip
from dweather_client.ipfs_errors import *
from dweather_client.utils import listify_period, celcius_to_fahrenheit
import dweather_client.ipfs_datasets
import csv

MM_TO_INCHES = 0.0393701
RAINFALL_PRECISION = 5
GATEWAY_URL = 'https://gateway.arbolmarket.com'


def get_heads(url=GATEWAY_URL):
    """
    Get heads.json for a given IPFS gateway.
    Args:
        url (str): base url of the IPFS gateway url
    Returns (example heads.json):
        {
            'chirps_05-daily': 'Qm...',
            'chirps_05-monthly': 'Qm...',
            'chirps_25-daily': 'Qm...',
            'chirps_25-monthly': 'Qm...',
            'cpc_us-daily': 'Qm...',
            'cpc_us-monthly': 'Qm...'
        }
    """
    hashes_url = url + "/climate/hashes/heads.json"
    r = requests.get(hashes_url)
    r.raise_for_status()
    return r.json()


def get_metadata(hash_str, url=GATEWAY_URL):
    """
    Get the metadata file for a given hash.
    Args:
        url (str): the url of the IPFS server
        hash_str (str): the hash of the ipfs dataset
    Returns (example metadata.json):
    
        {
            'date range': [
                '1981/01/01',
                '2019/07/31'
            ],
            'entry delimiter': ',',
            'latitude range': [
                -49.975, 49.975
            ],
            'longitude range': [
                -179.975, 179.975]
            ,
            'name': 'CHIRPS .05 Daily Full Set Uncompressed',
            'period': 'daily',
            'precision': 0.01,
            'resolution': 0.05,
            'unit of measurement': 'mm',
            'year delimiter': '\n'
        }
    """
    metadata_url = url + "/ipfs/" + hash_str + "/metadata.json"
    r = requests.get(metadata_url)
    r.raise_for_status()
    return r.json()


def get_station_csv(station_id):
    """
    Retrieve the contents of a station data csv file.
    Args:
        station_id (str): the id of the weather station
    returns:
        the contents of the station csv file as a string
    """
    all_hashes = get_heads()
    dataset_hash = all_hashes["ghcnd"]
    dataset_url = GATEWAY_URL + "/ipfs/" + dataset_hash + '/' + station_id + ".csv.gz"
    print(dataset_url)
    r = requests.get(dataset_url)
    print(r)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as zip_data:
        print(zip_data)
        return zip_data.read().decode("utf-8")


def parse_station_temps_as_dict(csv_text, use_fahrenheit=True):
    """
    Parse a station CSV file and get column values
    Will automatically index by date
    Args:
        csv_text (str): the GHCND station csv text
        use_fahrenheight (bool): if true use deg F, otherwise degrees C
    Returns:
        tuple:
            dict of datetime.date: float temperature highs
            dict of datetime.date: float temperature lows
    """
    #csv_text = get_station_csv(station_id)
    reader = csv.reader(csv_text.split())
    column_names = next(reader)
    date_col = column_names.index('DATE')
    tmax_col = column_names.index('TMAX')
    tmin_col = column_names.index('TMIN')
    tmins = {}
    tmaxs = {}
    for row in reader:
        # data is in tenths of a degree C
        if row[tmin_col] == '' or row[tmax_col] == '':
            continue
        tmax = float(row[tmax_col])/10.0
        tmin = float(row[tmin_col])/10.0
        if use_fahrenheit:
            tmax = celcius_to_fahrenheit(tmax)
            tmin = celcius_to_fahrenheit(tmin)
        tmaxs[datetime.datetime.strptime(row[date_col], "%Y-%m-%d").date()] = tmax
        tmins[datetime.datetime.strptime(row[date_col], "%Y-%m-%d").date()] = tmin

    return tmins, tmaxs



def get_station_by_wmo_id(wmo_id):
    pass

def get_station_by_airport_code(code):
    pass


def get_hash_cell(hash_str, coord_str):
    dataset_url = GATEWAY_URL + '/ipfs/' + hash_str + '/' + coord_str
    r = requests.get(dataset_url)
    r.raise_for_status()
    return r.text


def get_zipped_hash_cell(url, hash_str, coord_str):
    """
    Read a text file on the ipfs server compressed with gzip.
    Args:
        url (str): the url of the ipfs server
        hash_str (str): the hash of the dataset
        coord_str (str): the text file coordinate name e.g. 45.000_-96.000
    Returns:
        the contents of the file as a string
    """
    dataset_url = url + "/ipfs/" + hash_str + '/' + coord_str + ".gz"
    r = requests.get(dataset_url)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as zip_data:
        return zip_data.read().decode("utf-8")


def get_dataset_cell(lat, lon, dataset_revision):
    """ 
    Retrieve the text of a grid cell data file for a given lat lon and dataset.
    Args:
        lat (float): the latitude of the grid cell, to 3 decimals
        lon (float): the longitude of the grid cell, to 3 decimals
    Returns:
        A tuple (json, str) of the dataset metadata file and the grid cell data text
    Raises: 
        DatasetError: If no matching dataset found on server
        InputOutOfRangeError: If the lat/lon is outside the dataset range in metadata
        CoordinateNotFoundError: If the lat/lon coordinate is not found on server
    """
    all_hashes = get_heads()
    if dataset_revision in all_hashes:
        dataset_hash = all_hashes[dataset_revision]
    else:
        raise DatasetError('{} not found on server'.format(dataset_revision))

    metadata = get_metadata(dataset_hash)
    min_lat, max_lat = sorted(metadata["latitude range"])
    min_lon, max_lon = sorted(metadata["longitude range"])
    if lat < min_lat or lat > max_lat:
        raise InputOutOfRangeError("Latitude {} out of dataset revision range [{:.3f}, {:.3f}] for {}".format(lat, min_lat, max_lat, dataset_revision))
    if  lon < min_lon or lon > max_lon:
        raise InputOutOfRangeError("Longitude {} out of dataset revision range [{:.3f}, {:.3f}] for {}".format(lon, min_lon, max_lon, dataset_revision))
    coord_str = "{:.3f}_{:.3f}".format(lat,lon)
    try:
        if "compression" in metadata and metadata["compression"] == "gzip":
            text_data = get_zipped_hash_cell(GATEWAY_URL, dataset_hash, coord_str)
        else:
            text_data = get_hash_cell(dataset_hash, coord_str)
        return metadata, text_data
    except requests.exceptions.RequestException as e:
        raise CoordinateNotFoundError('Coordinate ({}, {}) not found  on ipfs in dataset revision {}'.format(lat, lon, dataset_revision))


def get_rainfall_dict(lat, lon, dataset_revision, return_metadata=False):
    """ 
    Build a dict of rainfall data for a given grid cell.
    Args:
        lat (float): the latitude of the grid cell, to 3 decimals
        lon (float): the longitude of the grid cell, to 3 decimals
    Returns:
        a dict ({datetime.date: float}) of datetime dates and the corresponding rainfall in mm for that date
    Raises:
        DatasetError: If no matching dataset found on server
        InputOutOfRangeError: If the lat/lon is outside the dataset range in metadata
        CoordinateNotFoundError: If the lat/lon coordinate is not found on server
        DataMalformedError: If the grid cell file can't be parsed as rainfall data
    """
    metadata, rainfall_text = get_dataset_cell(lat, lon, dataset_revision)
    dataset_start_date = datetime.datetime.strptime(metadata['date range'][0], "%Y/%m/%d").date()
    dataset_end_date = datetime.datetime.strptime(metadata['date range'][1], "%Y/%m/%d").date()
    timedelta = dataset_end_date - dataset_start_date
    days_in_record = timedelta.days + 1 # we have both the start and end date in the dataset so its the difference + 1
    day_strs = rainfall_text.replace(',', ' ').split()
    if (len(day_strs) != days_in_record):
        raise DataMalformedError ("Number of days in data file does not match the provided metadata")
    rainfall_dict = {}
    for i in range(days_in_record):
        if day_strs[i] == metadata["missing value"]:
            rainfall_dict[dataset_start_date + datetime.timedelta(days=i)] = None
        else:
            rainfall_dict[dataset_start_date + datetime.timedelta(days=i)] = float(day_strs[i])
    if return_metadata:
        return metadata, rainfall_dict
    else:
        return rainfall_dict


def get_rev_rainfall_dict(lat, lon, dataset, desired_end_date, latest_rev):
    """
    Build a dictionary of rainfall data. Include as much of the most accurate, final data as possible. Start by buidling from the most accurate data,
    then keep appending data from more recent/less accurate versions of the dataset until we run out or reach the end date.

    This will not throw an error if there are no revisions with data available, it will simply return what is available.
    Args:
        lat (float): the grid cell latitude
        lon (float): the grid cell longitude
        dataset (str): the name of the dataset, e.g., "chirps_05-daily" on hashes.json
        desired_end_date (datetime.date): the last day of data needed.
        latest_rev (str): the least accurate revision of the dataset that is considered final
    Returns:
        tuple:
            a dict ({datetime.date: float}) of datetime dates and the corresponding rainfall in mm for that date
            bool is_final: if all data up to desired end date is final, this will be true
    """
    all_rainfall = {}
    is_final = True

    # Build the rainfall from the most accurate revision of the dataset to the least
    for dataset_revision in dweather_client.ipfs_datasets.datasets[dataset]:
        additional_rainfall = get_rainfall_dict(lat, lon, dataset_revision)
        all_dates = list(all_rainfall) + list(additional_rainfall)
        # This method of dict comprehension preserves the order of the dict
        all_rainfall = {date: all_rainfall[date] if date in all_rainfall else additional_rainfall[date] for date in all_dates}
        # stop when we have the desired end date in the dataset
        if desired_end_date in all_rainfall:
            return all_rainfall, is_final
        # data is no longer final after we pass the specified version
        if dataset_revision == latest_rev:
            is_final = False

    # If we don't reach the desired dataset, return all data.
    return all_rainfall, is_final


def get_temperature_dict(lat, lon, dataset_revision, return_metadata=False):
    """
    Build a dict of temperature data for a given grid cell.
    Args:
        lat (float): the latitude of the grid cell, to 3 decimals
        lon (float): the longitude of the grid cell, to 3 decimals
    Returns:
        tuple (highs, lows) of dicts
        highs: dict ({datetime.date: float}) of datetime dates and the corresponding high temperature in degress F
        lows: dict ({datetime.date: float}) of datetime dates and the corresponding low temperature in degress F
    Raises:
        DatasetError: If no matching dataset_revision found on server
        InputOutOfRangeError: If the lat/lon is outside the dataset_revision range in metadata
        CoordinateNotFoundError: If the lat/lon coordinate is not found on server
        DataMalformedError: If the grid cell file can't be parsed as temperature data
    """
    metadata, temp_text = get_dataset_cell(lat, lon, dataset_revision)
    dataset_start_date = datetime.datetime.strptime(metadata['date range'][0], "%Y/%m/%d").date()
    dataset_end_date = datetime.datetime.strptime(metadata['date range'][1], "%Y/%m/%d").date()
    timedelta = dataset_end_date - dataset_start_date
    days_in_record = timedelta.days + 1 # we have both the start and end date in the dataset_revision so its the difference + 1
    day_strs = temp_text.replace(',', ' ').split()
    if (len(day_strs) != days_in_record):
        raise DataMalformedError ("Number of days in data file does not match the provided metadata")
    highs = {}
    lows = {}
    for i in range(days_in_record):
        low, high = map(float, day_strs[i].split('/'))
        date_iter = dataset_start_date + datetime.timedelta(days=i)
        highs[date_iter] = high
        lows[date_iter] = low
    if return_metadata:
        return metadata, highs, lows
    else:
        return highs, lows


def get_rev_temperature_dict(lat, lon, dataset, desired_end_date, latest_rev):
    """
    Build a dictionary of rainfall data. Include as much final data as possible. If the desired end date
    is not in the final dataset, append as much prelim as possible.
    Args:
        lat (float): the latitude of the grid cell, to 3 decimals
        lon (float): the longitude of the grid cell, to 3 decimals
        dataset (str): the dataset name as on hashes.json
        desired_end_date (datetime.date): don't include prelim data after this point if not needed
        latest_rev (str): The least accurate revision that is still considered 'final'
    returns:
        tuple (highs, lows) of dicts and a bool
        highs: dict ({datetime.date: float}) of datetime dates and the corresponding high temperature in degress F
        lows: dict ({datetime.date: float}) of datetime dates and the corresponding low temperature in degress F
        is_final: True if all data is from final dataset, false if prelim included
    """
    highs = {}
    lows = {}
    is_final = True

    # Build the data from the most accurate version of the dataset to the least
    for dataset_revision in dweather_client.ipfs_datasets.datasets[dataset]:
        additional_highs, additional_lows = get_temperature_dict(lat, lon, dataset_revision)
        all_dates = list(highs) + list(additional_highs)    
        highs = {date: highs[date] if date in highs else additional_highs[date] for date in all_dates}
        lows = {date: lows[date] if date in lows else additional_lows[date] for date in all_dates}
        # Stop early if we have the end date
        if desired_end_date in highs:
            return highs, lows, is_final

        # data is no longer final after we pass the specified version
        if dataset_revision == latest_rev:
            is_final = False

    # If we don't reach the desired dataset, return all data.
    return highs, lows, is_final


def get_rev_tagged_temperature_dict(lat, lon, dataset, desired_end_date=None):
    ''' Build temps with a revision tag by each date
    Args:
        lat (float): the grid cell latitude
        lon (float): the grid cell longitude
        dataset (str): the dataset name in ipfs
        desired_end_date (datetime.date): stop early if we get this end date
    returns
        highs: dict with keys of dates, values are tuple (temperature, revision tag) for that date
        lows: dict with keys of dates, values are tuple (temperature, revision tag) for that date
    '''
    highs = {}
    lows = {}

    # Build the data from the most accurate version of the dataset to the least
    for dataset_version in dweather_client.ipfs_datasets.datasets[dataset]:
        additional_highs, additional_lows = get_temperature_dict(lat, lon, dataset_version)
        all_dates = list(highs) + list(additional_highs)    
        highs = {date: highs[date] if date in highs else (additional_highs[date], dataset_version) for date in all_dates}
        lows = {date: lows[date] if date in lows else (additional_lows[date], dataset_version) for date in all_dates}
        # Stop early if we have the end date
        if desired_end_date in highs:
            return highs, lows

    # If we don't reach the desired dataset, return all data.
    return highs, lows


