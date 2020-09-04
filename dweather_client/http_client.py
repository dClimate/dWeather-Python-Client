"""
Basic functions for getting data from a dWeather gateway via https.
"""
import requests, datetime, io, gzip
from ipfs_errors import *
from utils import listify_period

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
    r = requests.get(dataset_url)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as zip_data:
        return zip_data.read().decode("utf-8")


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


def get_dataset_cell(lat, lon, dataset):
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
    if dataset in all_hashes:
        dataset_hash = all_hashes[dataset]
    else:
        raise DatasetError('{} not found on server'.format(dataset))

    metadata = get_metadata(dataset_hash)
    min_lat, max_lat = sorted(metadata["latitude range"])
    min_lon, max_lon = sorted(metadata["longitude range"])
    if lat < min_lat or lat > max_lat:
        raise InputOutOfRangeError("Latitude {} out of dataset range [{:.3f}, {:.3f}] for {}".format(lat, min_lat, max_lat, dataset))
    if  lon < min_lon or lon > max_lon:
        raise InputOutOfRangeError("Longitude {} out of dataset range [{:.3f}, {:.3f}] for {}".format(lon, min_lon, max_lon, dataset))
    coord_str = "{:.3f}_{:.3f}".format(lat,lon)
    try:
        if "compression" in metadata and metadata["compression"] == "gzip":
            text_data = get_zipped_hash_cell(server_url, dataset_hash, coord_str)
        else:
            text_data = get_hash_cell(dataset_hash, coord_str)
        return metadata, text_data
    except requests.exceptions.RequestException as e:
        raise CoordinateNotFoundError('Coordinate ({}, {}) not found  on ipfs in dataset {}'.format(lat, lon, dataset))


def get_rainfall_dict(lat, lon, dataset, return_metadata=False):
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
    metadata, rainfall_text = get_dataset_cell(lat, lon, dataset)
    dataset_start_date = datetime.datetime.strptime(metadata['date range'][0], "%Y/%m/%d").date()
    dataset_end_date = datetime.datetime.strptime(metadata['date range'][1], "%Y/%m/%d").date()
    timedelta = dataset_end_date - dataset_start_date
    days_in_record = timedelta.days + 1 # we have both the start and end date in the dataset so its the difference + 1
    day_strs = rainfall_text.replace(',', ' ').split()
    if (len(day_strs) != days_in_record):
        raise DataMalformedError ("Number of days in data file does not match the provided metadata")
    rainfall_dict = {}
    for i in range(days_in_record):
        rainfall_dict[dataset_start_date + datetime.timedelta(days=i)] = float(day_strs[i])
    if return_metadata:
        return metadata, rainfall_dict
    else:
        return rainfall_dict


def get_temperature_dict(lat, lon, dataset, return_metadata=False):
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
        DatasetError: If no matching dataset found on server
        InputOutOfRangeError: If the lat/lon is outside the dataset range in metadata
        CoordinateNotFoundError: If the lat/lon coordinate is not found on server
        DataMalformedError: If the grid cell file can't be parsed as temperature data
    """
    metadata, temp_text = get_dataset_cell(lat, lon, dataset)
    dataset_start_date = datetime.datetime.strptime(metadata['date range'][0], "%Y/%m/%d").date()
    dataset_end_date = datetime.datetime.strptime(metadata['date range'][1], "%Y/%m/%d").date()
    timedelta = dataset_end_date - dataset_start_date
    days_in_record = timedelta.days + 1 # we have both the start and end date in the dataset so its the difference + 1
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