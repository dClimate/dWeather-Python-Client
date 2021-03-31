"""
Queries associated with the https protocol option.
"""
import os, pickle, math, requests, datetime, io, gzip, json, logging, csv, tarfile
from collections import Counter, deque
from dweather_client.ipfs_errors import *
from dweather_client.aliases_and_units import FLASK_DATASETS

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
    metadata_url = "%s/ipfs/%s/metadata.json" % (url, hash_str)
    r = requests.get(metadata_url)
    r.raise_for_status()
    return r.json()

def get_hash_cell(hash_str, coord_str, url=GATEWAY_URL):
    """
    Read a text file from the gateway.
    Args:
        hash_str (str): the hash associated with the desired dataset
        coord_str (str): the string representation of the file 
        e.g. '45.000_-96.000'
    Returns:
        The contents of the file as a string
    """
    dataset_url = '%s/ipfs/%s/%s' % (url, hash_str, coord_str)
    r = requests.get(dataset_url)
    r.raise_for_status()
    return r.text

def get_zipped_hash_cell(hash_str, coord_str, url=GATEWAY_URL):
    """
    Read and decompress a text file from the gateway.
    Args:
        url (str): the url of the ipfs server
        hash_str (str): the hash of the dataset
        coord_str (str): the text file coordinate name e.g. 
        45.000_-96.000
    Returns:
        the contents of the file as a string
    """
    dataset_url = '%s/ipfs/%s/%s.gz' % (url, hash_str, coord_str)
    r = requests.get(dataset_url)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as zip_data:
        return zip_data.read().decode("utf-8")

def get_dataset_cell(lat, lon, dataset_revision, metadata=None):
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
    if metadata is None:
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
            text_data = get_zipped_hash_cell(dataset_hash, coord_str)
        else:
            text_data = get_hash_cell(dataset_hash, coord_str)
        return text_data
    except requests.exceptions.RequestException as e:
        raise CoordinateNotFoundError('Coordinate ({}, {}) not found  on ipfs in dataset revision {}'.format(lat, lon, dataset_revision))

def get_station_csv(station_id, station_dataset="ghcnd-imputed-daily", url=GATEWAY_URL):
    """
    Retrieve the contents of a station data csv file.
    Args:
        station_id (str): the id of the weather station
        station_dataset (str): which dataset to use, on of ["ghcnd", "ghcnd-imputed-daily"]
    returns:
        the contents of the station csv file as a string
    """
    all_hashes = get_heads()
    dataset_hash = all_hashes[station_dataset]
    dataset_url = "%s/ipfs/%s/%s.csv.gz" % (url, dataset_hash, str(station_id))
    r = requests.get(dataset_url)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as zip_data:
        return zip_data.read().decode("utf-8")

def get_hurricane_release_dict(release_hash, url=GATEWAY_URL):
    url = "%s/ipfs/%s/history.json.gz" % (url, release_hash)
    resp = requests.get(url)
    resp.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as zip_data:
        return json.loads(zip_data.read().decode("utf-8"))

def get_hurricane_dict(head=None):
    """
    Get a hurricane dictionary for the atcf_btk-seasonal dataset. 
    To get a unique value to query the dict by storm, use BASIN + CY + the year
    part of the HOUR value. BASIN is the ocean, CY is the storm index, and
    the year is needed as well because the storm index resets every year.
    Note that there will be multiple readings with the same HOUR value,
    as readings are taken more than once per hour and then rounded to the nearest
    hour before posting. 
    """
    heads = get_heads()
    hurr_head = heads['atcf_btk-seasonal']
    release_ll = traverse_ll(hurr_head)
    hurr_dict = {}
    for release_hash in release_ll:
        release_content = get_hurricane_release_dict(release_hash)
        try:
            hurr_dict['features'] += release_content['features']
        except KeyError:
            hurr_dict.update(release_content)
    return hurr_dict

def get_simulated_hurricane_files(basin):
    """
    Gets the names of files containing STORM simulated TC data. Takes a basin ID, one of:
    EP, NA, NI, SI, SP or WP
    """
    if basin not in {'EP', 'NA', 'NI', 'SI', 'SP', 'WP'}:
        raise ValueError("Invalid basin ID")
    heads = get_heads()
    hurr_hash = heads['storm-simulated-hurricane']
    metadata  = get_metadata(hurr_hash)
    base_url = f"{GATEWAY_URL}/ipfs/{hurr_hash}/"
    files = [base_url + f for f in metadata['files'] if basin in f]
    return files

def get_ibracs_hurricane_file():
    heads = get_heads()
    hurr_hash = heads['ibtracs-tropical-storm']
    metadata = get_metadata(hurr_hash)
    base_url = f"{GATEWAY_URL}/ipfs/{hurr_hash}/"
    return base_url + metadata["files"][0]

def traverse_ll(head):
    release_itr = head
    release_ll = deque()
    while True:
        release_ll.appendleft(release_itr)
        prev_release = get_metadata(release_itr)['previous hash']
        if prev_release != None:
            release_itr = prev_release
        else:
            return release_ll

def flask_query(dataset, lat, lon, base_url=GATEWAY_URL):
    if dataset not in FLASK_DATASETS:
        raise ValueError(f"Valid flask datasets are {FLASK_DATASETS}")

    url = f"{base_url}/linked-list/{dataset}/{lat}_{lon}"
    r = requests.get(url)
    r.raise_for_status()
    resp = r.json()

    data_dict = {}
    if "hourly" in dataset:
        for k, v in resp["data"].items():
            data_dict[datetime.datetime.fromisoformat(k)] = v
    elif "daily" in dataset:
        for k, v in resp["data"].items():
            data_dict[datetime.datetime.fromisoformat(k).date()] = v

    return ((resp["lat"], resp["lon"]), data_dict)
