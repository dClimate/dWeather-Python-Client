"""
Basic functions for getting data from a dWeather gateway via https.
"""
import os, pickle, math, requests, datetime, io, gzip, json, logging, csv, tarfile, pint
from collections import Counter, deque
from dweather_client.ipfs_errors import *
from dweather_client.utils import *
import dweather_client.ipfs_datasets
from depreciated import *

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
            text_data = get_zipped_hash_cell(dataset_hash, coord_str)
        else:
            text_data = get_hash_cell(dataset_hash, coord_str)
        return metadata, text_data
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

def get_station_col_as_dict(station_id, columns, dataset='ghcnd', use_imperial=True):
    """
    Takes in a station id and a column name or iterable of of column names. 

    Gets the csv body associated with the station_id, defaulting to the
    ghcnd dataset. Pass in dataset='ghcnd-imputed-daily' for imputed,
    though note that this is temperature only as of this writing.

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
    column_lookup = { \
        ('SNWD', 
            'snow depth', 
                'snowdepth'): ('SNWD',),
        ('SNOW', 
            'snow fall', 
                'snowfall', 
                    'snow'): ('SNOW',),
        ('WESD', 
            'snow water equivalent',
                'water equivalent snow depth'): ('WESD',),
        ('TMAX', 
            'highs', 
                'max temperature', 
                    'temperature max', 
                        'maximum temperature', 
                            'temperature maximum',
                                'max temp', 
                                    'temp max', 
                                        'maximum temp', 
                                            'temp maximum'): ('TMAX',),
        ('TMIN', 
            'lows', 
                'min temperature', 
                    'temperature min' 
                        'minimum temperature', 
                            'temperature minimum',
                                'min temp', 
                                    'temp min', 
                                        'minimum temp', 
                                            'temp minimum'): ('TMIN',),
        ('temperature', 
            'temperatures', 
                'temp', 
                    'temps'): ('TMAX', 'TMIN'),
        ('PRCP', 
            'precipitation', 
                'precip', 
                    'rain', 
                        'rainfall': ('PRCP',))
    }

    units = pint.UnitRegistry()
    units.default_format = '.3f' # 3 units of precision
    units_lookup = { \
        'PRCP': {'imperial': units.inch, 'metric': units.mm},
        'SNWD': {'imperial': units.inch, 'metric': units.mm},
        'SNOW': {'imperial': units.inch, 'metric': units.mm},
        'WESD': {'imperial': units.inch, 'metric': units.mm},
        'TMAX': {'imperial': units.degF, 'metric': units.degC},
        'TMIN': {'imperial': units.degF, 'metric': units.degC},
    }

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

def get_rainfall_dict(lat, lon, dataset_revision, return_metadata=False, get_counter=False):
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
    rainfall_dict = Counter({}) if get_counter else {}
    for i in range(days_in_record):
        if day_strs[i] == metadata["missing value"]:
            rainfall_dict[dataset_start_date + datetime.timedelta(days=i)] = 0 if get_counter else None
        else:
            rainfall_dict[dataset_start_date + datetime.timedelta(days=i)] = float(day_strs[i])
    if return_metadata:
        return metadata, rainfall_dict
    else:
        return rainfall_dict

def get_hurricane_dict(head=get_heads()['atcf_btk-seasonal']):
    """
    Get a hurricane dictionary for the atcf_btk-seasonal dataset. 

    To get a unique value to query the dict by storm, use BASIN + CY + the year
    part of the HOUR value. BASIN is the ocean, CY is the storm index, and
    the year is needed as well because the storm index resets every year.

    Note that there will be multiple readings with the same HOUR value,
    as readings are taken more than once per hour and then rounded to the nearest
    hour before posting. 
    """
    release_ll = traverse_ll(head)
    hurr_dict = {}
    for release_hash in release_ll:
        url = "%s/ipfs/%s/history.json.gz" % (url, release_hash)
        resp = requests.get(url)
        resp.raise_for_status()
        with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as zip_data:
            release_content =json.loads(zip_data.read().decode("utf-8"))
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


def get_full_rtma_history(lat, lon):
    """
    Calls endpoint that iterates through all updates to the RTMA dataset and returns a dictionary
    containing the full time series of data.
    Args:
        lat (float): latitude coordinate of RTMA data
        lon (float): longitude coordinate of RTMA data
    Returns:
        tuple containing (<ret_lat>, <ret_lon>, <data>)
        where ret_lat and ret_lon are floats representing the coordinates of the data after the
        argument coordinates are snapped to the RTMA grid, and <data> is a time series dict with 
        datetime keys
    """
    if ((lat < 20) or (53 < lat)):
        raise InputOutOfRangeError('RTMA only covers latitudes 20 thru 53')
    if ((lon < -132) or (-60 < lon)):
        raise InputOutOfRangeError('RTMA only covers longitudes -132 thru -60')
    base_url = "https://parser.arbolmarket.com/linked-list/rtma"
    r = requests.get(f"{base_url}/{lat}_{lon}")
    resp = r.json()
    data_dict = {}
    for k, v in resp["data"].items():
        data_dict[datetime.datetime.fromisoformat(k)] = v
    return ((resp["lat"], resp["lon"]), data_dict)

def get_prismc_dict(lat, lon, dataset):
    """
    Builds a dict of latest PRISM data by using datasets combining all PRISM revisions
    Args:
        lat (float): the latitude of the grid cell, to 3 decimals
        lon (float): the longitude of the grid cell, to 3 decimals
        dataset (str): one of 'precip', 'tmax' or 'tmin'
    Returns:
        a dict ({datetime.date: float}) of datetime dates and the corresponding weather values.
        Units are mm for precip or degrees F for tmax and tmin
    """
    if dataset not in {"precip", "tmax", "tmin"}:
        raise ValueError("Dataset must be 'precip', 'tmax' or 'tmin'")
    str_lat, str_lon = "{:.3f}".format(lat), "{:.3f}".format(lon)
    prismc_head = get_heads()[f"prismc-{dataset}-daily"]
    date_dict = {}
    hashes = traverse_ll(prismc_head)
    for h in list(hashes)[::-1]:
        tar_url = f"{GATEWAY_URL}/ipfs/{h}/{str_lat}.tar"
        resp = requests.get(tar_url)
        resp.raise_for_status()
        with tarfile.open(fileobj=io.BytesIO(resp.content)) as tar:
            with tar.extractfile(f"{str_lat}_{str_lon}.gz") as f:
                with gzip.open(f) as gz:
                    for i, line in enumerate(gz):
                        day_of_year = datetime.date(1981 + i, 1, 1)
                        data_list = line.decode('utf-8').strip().split(',')
                        for point in data_list:
                            if (day_of_year not in date_dict) and point:
                                date_dict[day_of_year] = float(point)
                                day_of_year += datetime.timedelta(days=1)
    return date_dict

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

def get_era5_dict(lat, lon, dataset):
    """
    Builds a dict of era5 data
    Args:
        lat (float): the latitude of the grid cell. Will be rounded to one decimal
        lon (float): the longitude of the grid cell. Will be rounded to one decimal
        dataset (str): valid era5 dataset. Currently only 'era5_land_wind_u-hourly', but
        more will be added to ipfs soon
    Returns:
        a dict ({datetime.datetime: float}) of datetimes and the corresponding weather values.
        Units are m/s for the wind datasets
    """
    heads = get_heads()
    era5_hash = heads[dataset]

    snapped_lat, snapped_lon = round(lat, 1), round(lon, 1)
    cpc_lat, cpc_lon = conventional_lat_lon_to_cpc(snapped_lat, snapped_lon)
    formatted_lat, formatted_lon = f"{cpc_lat:08.3f}", f"{cpc_lon:08.3f}"
    url = f"{GATEWAY_URL}/ipfs/{era5_hash}/{formatted_lat}_{formatted_lon}.gz"
    resp = requests.get(url)
    resp.raise_for_status()
    datetime_dict = {}
    with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as gz:
        for i, line in enumerate(gz):
            time_of_year = datetime.datetime(1990 + i, 1, 1)
            data_list = line.decode('utf-8').strip().split(',')
            for point in data_list:
                datetime_dict[time_of_year] = float(point)
                time_of_year += datetime.timedelta(hours=1)
    return (snapped_lat, snapped_lon), datetime_dict





