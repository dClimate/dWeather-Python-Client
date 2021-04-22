"""
Queries associated with the ipfs protocol option.
"""

from abc import ABC, abstractmethod
from collections import deque
import ipfshttpclient, json, datetime, io, os, tarfile, gzip, pickle
from dweather_client.ipfs_errors import *
from dweather_client.grid_utils import conventional_lat_lon_to_cpc, cpc_lat_lon_to_conventional
from dweather_client.struct_utils import find_closest_lat_lon
from dweather_client.http_queries import get_heads
import pandas as pd
from io import BytesIO


METADATA_FILE = "metadata.json"

class IpfsDataset(ABC):
    @property
    @abstractmethod
    def dataset(self):
        pass

    def __init__(self, ipfs_timeout=None):
        self.head = get_heads()[self.dataset]
        self.offline = not ipfs_timeout
        self.ipfs = ipfshttpclient.connect(timeout=ipfs_timeout, offline=self.offline)

    def get_metadata(self, h):
        metadata = self.ipfs.cat(f"{h}/{METADATA_FILE}").decode('utf-8')
        return json.loads(metadata)

    def get_file_object(self, f):
        return BytesIO(self.ipfs.cat(f))

    @abstractmethod
    def get_data(*args, **kwargs):
        pass

class GriddedDataset(IpfsDataset):
    @classmethod
    def snap_to_grid(cls, lat, lon, metadata):
        """
        Find the nearest (lat,lon) on IPFS for a given metadata file.
        return: lat, lon
        args:
            lat = -90 < lat < 90, float
            lon = -180 < lon < 180, float
            metadata: a dWeather metadata file

        """
        resolution = metadata['resolution']
        min_lat = metadata['latitude range'][0]  # start [lat, lon]
        min_lon = metadata['longitude range'][0]  # end [lat, lon]

        # check that the lat lon is in the bounding box
        snap_lat = round(round((lat - min_lat)/resolution) * resolution + min_lat, 3)
        snap_lon = round(round((lon - min_lon)/resolution) * resolution + min_lon, 3)
        return snap_lat, snap_lon

    def traverse_ll(self, head):
        release_itr = head
        release_ll = deque()
        while True:
            release_ll.appendleft(release_itr)
            try:
                prev_release = self.get_metadata(release_itr)['previous hash']
            except KeyError:
                return release_ll
            if prev_release is not None:
                release_itr = prev_release
            else:
                return release_ll

    def get_hashes(self):
        hashes = self.traverse_ll(self.head)
        return list(hashes)

    def get_date_range_from_metadata(self, h):
        metadata = self.get_metadata(h)
        return (metadata["date range"][0], metadata["date range"][1])

    def get_weather_dict(self, date_range, ipfs_hash, root):
        if not root:
            with tarfile.open(fileobj=self.get_file_object(f"{ipfs_hash}/{self.tar_name}")) as tar:
                member = tar.getmember(self.gzip_name)
                with gzip.open(tar.extractfile(member), "rb") as gz:
                    cell_text = gz.read().decode('utf-8')
        else:
            with gzip.open(self.get_file_object(f"{ipfs_hash}/{self.gzip_name}")) as gz:
                cell_text = gz.read().decode('utf-8')

        day_itr = date_range[0]
        weather_dict = {}
        if "daily" in self.dataset:
            for year_data in cell_text.split('\n'):
                for day_data in year_data.split(','):
                    weather_dict[day_itr.date().isoformat()] = day_data
                    day_itr = day_itr + datetime.timedelta(days=1)
        elif "hourly" in self.dataset:
            for year_data in cell_text.split('\n'):
                for hour_data in year_data.split(','):
                    weather_dict[day_itr.isoformat()] = hour_data
                    day_itr = day_itr + datetime.timedelta(hours=1)
        return weather_dict

class RtmaGriddedDataset(GriddedDataset):
    _parent_dir = os.path.dirname(os.path.abspath(__file__))
    CHUNKS = os.path.join(_parent_dir, 'etc', 'rtma_chunks.txt')
    VALID_COORDS = os.path.join(_parent_dir, 'etc', 'rtma_valid_coordinates.txt')
    GRID_MAPPING = os.path.join(_parent_dir, 'etc', 'rtma_grid_mapping.p.gz')
    COORD_BUCKETS = os.path.join(_parent_dir, 'etc', 'rtma_lat_lons.p.gz')
    CHUNK_SIZE = 1000

    def get_data(self, lat, lon):
        self.get_grid_x_y(lat, lon)
        self.str_x, self.str_y = f'{self.x_grid:04}', f'{self.y_grid:04}'
        index = self.get_file_index()
        self.tar_name = self.find_tar(index)
        self.gzip_name = f"{self.str_x}_{self.str_y}.gz"
        ret_dict = {}
        for i, h in enumerate(self.get_hashes()):
            date_range = [datetime.datetime.fromisoformat(dt) for dt in self.get_date_range_from_metadata(h)]
            rtma_dict = self.get_weather_dict(date_range, h, i == 0)
            ret_dict = {**ret_dict, **rtma_dict}
        ret_lat, ret_lon = cpc_lat_lon_to_conventional(self.snapped_lat, self.snapped_lon)
        return {"lat": float(ret_lat), "lon": float(ret_lon), "data": ret_dict}

    def get_grid_x_y(self, lat, lon):
        lat, lon = conventional_lat_lon_to_cpc(lat, lon)
        if ((lat < 20) or (53 < lat)):
            raise FileNotFoundError('RTMA only covers latitudes 20 thru 53')
        if ((lon < 228) or (300 < lon)):
            raise FileNotFoundError('RTMA only covers longitudes -132 thru -60')
        lat, lon = str(lat), str(lon)

        with gzip.open(self.COORD_BUCKETS) as f:
            valid_lat_lons = pickle.load(f)
        close_array = valid_lat_lons[lat[:2], lon[:3]]
        closest = find_closest_lat_lon(close_array, (lat, lon))

        with gzip.open(self.GRID_MAPPING) as f:
            grid_dict = pickle.load(f)
        grid = grid_dict[closest[0]]

        self.x_grid = grid[0]
        self.y_grid = grid[1]
        self.snapped_lat = closest[0]
        self.snapped_lon = closest[1]

    def get_file_index(self):
        with open(self.VALID_COORDS, "r") as f:
            for i, line in enumerate(f):
                if line.strip() == str((self.x_grid, self.y_grid)):
                    return i

    def find_tar(self, index):
        with open(self.CHUNKS) as f:
            balls = [line.strip() for line in list(f)]
        ball_index = index // self.CHUNK_SIZE
        return balls[ball_index]

class RtmaWindUHourly(RtmaGriddedDataset):
    @property
    def dataset(self):
        return "rtma_wind_u-hourly"

class SimpleGriddedDataset(GriddedDataset):
    SIG_DIGITS = 3

    @property
    def zero_padding(self):
        return None

    def get_file_names(self):
        if self.zero_padding:
            lat_portion = f"{self.snapped_lat:0{self.zero_padding}.{self.SIG_DIGITS}f}"
            lon_portion = f"{self.snapped_lon:0{self.zero_padding}.{self.SIG_DIGITS}f}"
        else:
            lat_portion = f"{self.snapped_lat:.{self.SIG_DIGITS}f}"
            lon_portion = f"{self.snapped_lon:.{self.SIG_DIGITS}f}"
        return {
            "tar": f"{lat_portion}.tar",
            "gz": f"{lat_portion}_{lon_portion}.gz"
        }

    def get_data(self, lat, lon):
        first_metadata = self.get_metadata(self.head)
        if "cpcc" in self.dataset or "era5" in self.dataset:
            lat, lon = conventional_lat_lon_to_cpc(float(lat), float(lon))
        self.snapped_lat, self.snapped_lon = self.snap_to_grid(float(lat), float(lon), first_metadata)
        self.tar_name = self.get_file_names()["tar"]
        self.gzip_name = self.get_file_names()["gz"]
        ret_dict = {}
        for i, h in enumerate(self.get_hashes()):
            date_range = [datetime.datetime.fromisoformat(dt) for dt in self.get_date_range_from_metadata(h)]
            weather_dict = self.get_weather_dict(date_range, h, i == 0)
            ret_dict = {**ret_dict, **weather_dict}
        ret_lat, ret_lon = cpc_lat_lon_to_conventional(self.snapped_lat, self.snapped_lon)
        return {"lat": ret_lat, "lon": ret_lon, "data": ret_dict}

class ChirpscFinal05Daily(SimpleGriddedDataset):
    @property
    def dataset(self):
        return "chirpsc_final_05-daily"

class Era5LandWind(SimpleGriddedDataset):
    @property
    def zero_padding(self):
        return 8

class Era5LandWindUHourly(Era5LandWind):
    @property
    def dataset(self):
        return "era5_land_wind_u-hourly"

def pin_all_stations(client=None, station_dataset="ghcnd-imputed-daily"):
    """ Sync all stations locally."""
    heads = get_heads()
    dataset_hash = heads[station_dataset]
    session_client = ipfshttpclient.connect() if client is None else client
    try:
        session_client.pin.add(dataset_hash)
    finally:
        if (client is None):
            session_client.close()

def cat_station_df(station_id, station_dataset="ghcnd-imputed-daily", client=None, pin=True, force_hash=None):
    """ Cat a given station's raw data as a pandas dataframe. """
    df = pd.read_csv(io.StringIO(\
        cat_station_csv(
            station_id,
            station_dataset=station_dataset,
            client=client, 
            pin=pin,
            force_hash=force_hash
        )
    ))
    return df.set_index(pd.DatetimeIndex(df['DATE']))

def cat_station_csv(station_id, station_dataset="ghcnd-imputed-daily", client=None, pin=True, force_hash=None):
    """
    Retrieve the contents of a station data csv file.
    Args:
        station_id (str): the id of the weather station
        station_dataset(str): on of ["ghcnd", "ghcnd-imputed-daily"]
    returns:
        the contents of the station csv file as a string
    """
    if (force_hash is None):
        all_hashes = get_heads()
        dataset_hash = all_hashes[station_dataset]
    else:
        dataset_hash = force_hash
    csv_hash = dataset_hash + '/' + station_id + ".csv.gz"
    session_client = ipfshttpclient.connect() if client is None else client
    try:
        if pin:
            session_client.pin.add(csv_hash)
        csv = session_client.cat(csv_hash)
        with gzip.GzipFile(fileobj=io.BytesIO(csv)) as zip_data:
            return zip_data.read().decode("utf-8")
    finally:
        if (client is None):
            session_client.close()

def cat_icao_stations(station_dataset="ghcnd-imputed-daily", pin=True, force_hash=None):
    """
    For every station that has an icao code, load it into a dataframe and
    return them all as a list.
    """
    station_ids = get_station_ids_with_icao()
    return cat_station_df_list(station_ids, station_dataset=station_dataset, pin=pin, force_hash=force_hash)

def cat_n_closest_station_dfs(lat, lon, n, station_dataset="ghcnd-imputed-daily", pin=True, force_hash=None):
    """
    Load the closest n stations to a given point into a list of dataframes.
    """
    if (force_hash is None):
        metadata = cat_metadata(get_heads()[station_dataset])
    else:
        metadata = cat_metadata(force_hash)
    station_ids = get_n_closest_station_ids(lat, lon, metadata, n)
    return cat_station_df_list(station_ids, station_dataset=station_dataset, pin=pin, force_hash=force_hash)

def cat_station_df_list(station_ids, station_dataset="ghcnd-imputed-daily", pin=True, force_hash=None):
    batch_hash = force_hash
    if (force_hash is None):
        batch_hash = get_heads()[station_dataset]
    metadata = cat_metadata(batch_hash, pin=pin)
    station_content = []
    with ipfshttpclient.connect() as client:
        for station_id in station_ids:
            logging.info("(%i of %i): Loading station %s from %s into DataFrame%s" % ( \
                station_ids.index(station_id) + 1,
                len(station_ids),
                station_id, 
                "dWeather head" if force_hash is None else "forced hash",
                " and pinning to ipfs datastore" if pin else ""
            ))
            try:
                station_content.append(cat_station_df( \
                    station_id,
                    station_dataset=station_dataset,
                    client=client,
                    pin=pin,
                    force_hash=batch_hash
            ))
            except ipfshttpclient.exceptions.ErrorResponse:
                logging.warning("Station %s not found" % station_id)
                
    return station_content 


def cat_icao_stations(client=None, pin=True):
    """ Get a list of station dataframes for all stations that have an icao"""
    dfs = []
    session_client = ipfshttpclient.connect() if client is None else client
    try:
        for station_id in get_station_ids_with_icao():
            try:
                print(station_id)
                dfs.append(cat_station_csv(station_id, client=client, pin=pin))
            except Exception as e:
                print(e)
                continue
    finally:
        if (client is None):
            session_client.close()
    return dfs


if __name__ == "__main__":
    dataset = Era5LandWindUHourly(ipfs_timeout=10)
    d = dataset.get_data(40, -120)
    import ipdb; ipdb.set_trace()