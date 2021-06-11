"""
Queries associated with the ipfs protocol option.
"""

from abc import ABC, abstractmethod
from collections import deque
import ipfshttpclient, json, datetime, os, tarfile, gzip, pickle, zipfile
from dweather_client.ipfs_errors import *
from dweather_client.grid_utils import conventional_lat_lon_to_cpc, cpc_lat_lon_to_conventional
from dweather_client.struct_utils import find_closest_lat_lon
from dweather_client.http_queries import get_heads
import pandas as pd
from io import BytesIO


METADATA_FILE = "metadata.json"
GATEWAY_IPFS_ID = "/ip4/134.122.126.13/tcp/4001/p2p/12D3KooWM8nN6VbUka1NeuKnu9xcKC56D17ApAVRDyfYNytzUsqG"

class IpfsDataset(ABC):
    """
    Base class for handling requests for all IPFS datasets
    """
    @property
    @abstractmethod
    def dataset(self):
        """
        Dataset name must be overwritten by leaf class in order to be instantiable
        """
        pass

    def __init__(self, ipfs_timeout=None):
        """
        args:
        :ipfs_timeout: Time IPFS should wait for response before throwing exception. If None, will assume that
        code is running in an environment containing all datasets (such as gateway)
        """
        self.on_gateway = not ipfs_timeout
        self.ipfs = ipfshttpclient.connect(timeout=ipfs_timeout)

    def get_metadata(self, h):
        """
        args:
        :h: dClimate IPFS hash from which to get metadata
        return:
            metadata as dict
        """
        if not self.on_gateway:
            self.ipfs._client.request('/swarm/connect', (GATEWAY_IPFS_ID,))
        metadata = self.ipfs.cat(f"{h}/{METADATA_FILE}").decode('utf-8')
        return json.loads(metadata)

    def get_file_object(self, f):
        """
        args:
        :h: dClimate IPFS hash from which to get data. Must point to a file, not a directory
        return:
            content of file as file-like bytes object
        """
        if not self.on_gateway:
            self.ipfs._client.request('/swarm/connect', (GATEWAY_IPFS_ID,))
        return BytesIO(self.ipfs.cat(f))

    def traverse_ll(self, head):
        """
        Iterates through a linked list of metadata files
        args:
        :head: ipfs hash of the directory at the head of the linked list
        return: deque containing all hashes in the linked list
        """
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

    @abstractmethod
    def get_data(self, *args, **kwargs):
        """
        Exposed method that allows user to get data in the dataset. Args and return value will depend on whether
        this is a gridded, station or storm dataset
        """
        self.head = get_heads()[self.dataset]

class GriddedDataset(IpfsDataset):
    """
    Abstract class from which all gridded, linked list datasets inherit
    """
    @classmethod
    def snap_to_grid(cls, lat, lon, metadata):
        """
        Find the nearest (lat,lon) on IPFS for a given metadata file.
        args:
        :lat: = -90 < lat < 90, float
        :lon: = -180 < lon < 180, float
        :metadata: a dWeather metadata file
        return: lat, lon
        """
        resolution = metadata['resolution']
        min_lat = metadata['latitude range'][0]  # start [lat, lon]
        min_lon = metadata['longitude range'][0]  # end [lat, lon]

        # check that the lat lon is in the bounding box
        snap_lat = round(round((lat - min_lat) / resolution) * resolution + min_lat, 3)
        snap_lon = round(round((lon - min_lon) / resolution) * resolution + min_lon, 3)
        return snap_lat, snap_lon

    def get_hashes(self):
        """
        return: list of all hashes in dataset
        """
        hashes = self.traverse_ll(self.head)
        return list(hashes)

    def get_date_range_from_metadata(self, h):
        """
        args:
        :h: hash for ipfs directory containing metadata
        return: list of [start_time, end_time]
        """
        metadata = self.get_metadata(h)
        str_dates = (metadata["date range"][0], metadata["date range"][1])
        return [datetime.datetime.fromisoformat(dt) for dt in str_dates]

    def get_weather_dict(self, date_range, ipfs_hash, is_root):
        """
        Get a pd.Series of weather values for a given IPFS hash
        args:
        :date_range: time range that hash has data for
        :ipfs_hash: hash containing data
        :is_root: bool indicating whether this is the root node in the linked list
        return: pd.Series with date or datetime index and weather values
        """
        if not is_root:
            try:
                with tarfile.open(fileobj=self.get_file_object(f"{ipfs_hash}/{self.tar_name}")) as tar:
                    member = tar.getmember(self.gzip_name)
                    with gzip.open(tar.extractfile(member)) as gz:
                        cell_text = gz.read().decode('utf-8')
            except ipfshttpclient.exceptions.ErrorResponse:
                zip_file_name = self.tar_name[:-4] + '.zip'
                with zipfile.ZipFile(self.get_file_object(f"{ipfs_hash}/{zip_file_name}")) as zi:
                    with gzip.open(zi.open(self.gzip_name)) as gz:
                        cell_text = gz.read().decode('utf-8')
        else:
            with gzip.open(self.get_file_object(f"{ipfs_hash}/{self.gzip_name}")) as gz:
                cell_text = gz.read().decode('utf-8')

        day_itr = date_range[0]
        weather_dict = {}
        if "daily" in self.dataset:
            for year_data in cell_text.split('\n'):
                for day_data in year_data.split(','):
                    weather_dict[day_itr.date()] = day_data
                    day_itr = day_itr + datetime.timedelta(days=1)
        elif "hourly" in self.dataset:
            for year_data in cell_text.split('\n'):
                for hour_data in year_data.split(','):
                    weather_dict[day_itr] = hour_data
                    day_itr = day_itr + datetime.timedelta(hours=1)
        return weather_dict

class PrismGriddedDataset(GriddedDataset):
    """
    Abstract class from which all PRISM datasets inherit. Contains logic for overlapping date ranges
    that is unique to PRISM
    """
    def get_data(self, lat, lon):
        """
        PRISM datasets' method for getting data. Reverses the linked list so as to correctly prioritize displaying
        more recent data
        args:
        :lat: float of latitude from which to get data
        :lon: float of longitude from which to get data
        return: tuple of lat/lon snapped to PRISM grid, and weather data, which is pd.Series with date index
        and str values corresponding to weather observations
        """
        super().get_data()
        first_metadata = self.get_metadata(self.head)
        snapped_lat, snapped_lon = self.snap_to_grid(float(lat), float(lon), first_metadata)
        self.tar_name = f"{snapped_lat:.3f}.tar"
        self.gzip_name = f"{snapped_lat:.3f}_{snapped_lon:.3f}.gz"
        self.ret_dict = {}
        for h in self.get_hashes()[::-1]:
            self.update_prismc_dict(h)
        return (float(snapped_lat), float(snapped_lon)), pd.Series(self.ret_dict)

    def update_prismc_dict(self, ipfs_hash):
        """
        Updates self.ret_dict with data from a hash in the linked list. Written so as to never
        overwrite newer data with older
        args:
        :ipfs_hash: hash in linked list from which to get data
        """
        try:
            with tarfile.open(fileobj=self.get_file_object(f"{ipfs_hash}/{self.tar_name}")) as tar:
                member = tar.getmember(self.gzip_name)
                with gzip.open(tar.extractfile(member), "rb") as gz:
                    for i, line in enumerate(gz):
                        day_of_year = datetime.date(1981 + i, 1, 1)
                        data_list = line.decode('utf-8').strip().split(',')
                        for point in data_list:
                            if (day_of_year not in self.ret_dict) and point:
                                self.ret_dict[day_of_year] = point
                            day_of_year += datetime.timedelta(days=1)
                        
        except ipfshttpclient.exceptions.ErrorResponse:
            zip_file_name = self.tar_name[:-4] + '.zip'
            with zipfile.ZipFile(self.get_file_object(f"{ipfs_hash}/{zip_file_name}")) as zi:
                with gzip.open(zi.open(self.gzip_name), "rb") as gz:
                    for i, line in enumerate(gz):
                        day_of_year = datetime.date(1981 + i, 1, 1)
                        data_list = line.decode('utf-8').strip().split(',')
                        for point in data_list:
                            if (day_of_year not in self.ret_dict) and point:
                                self.ret_dict[day_of_year] = point
                            day_of_year += datetime.timedelta(days=1)

class RtmaGriddedDataset(GriddedDataset):
    """
    Abstract class from which RTMA datasets inherits. Contains custom logic for converting lat/lons to 
    RTMAs unique gridding system
    """
    _etc_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "etc")
    CHUNKS = os.path.join(_etc_dir, 'rtma_chunks.txt')
    VALID_COORDS = os.path.join(_etc_dir, 'rtma_valid_coordinates.txt')
    GRID_MAPPING = os.path.join(_etc_dir, 'rtma_grid_mapping.p.gz')
    COORD_BUCKETS = os.path.join(_etc_dir, 'rtma_lat_lons.p.gz')
    CHUNK_SIZE = 1000

    def get_data(self, lat, lon):
        """
        RTMA datasets' method for getting data.
        args:
        :lat: float of latitude from which to get data
        :lon: float of longitude from which to get data
        return: tuple of lat/lon snapped to RTMA grid, and weather data, which is pd.Series with datetime index
        and str values corresponding to weather observations
        """
        super().get_data()
        (self.x_grid, self.y_grid), (self.snapped_lat, self.snapped_lon) = self.get_grid_x_y(lat, lon)
        str_x, str_y = f'{self.x_grid:04}', f'{self.y_grid:04}'
        index = self.get_file_index()
        self.tar_name = self.find_archive(index)
        self.gzip_name = f"{str_x}_{str_y}.gz"
        ret_dict = {}
        for i, h in enumerate(self.get_hashes()):
            date_range = self.get_date_range_from_metadata(h)
            rtma_dict = self.get_weather_dict(date_range, h, i == 0)
            ret_dict = {**ret_dict, **rtma_dict}
        ret_lat, ret_lon = cpc_lat_lon_to_conventional(self.snapped_lat, self.snapped_lon)
        return (float(ret_lat), float(ret_lon)), pd.Series(ret_dict)

    def get_grid_x_y(self, lat, lon):
        """
        Converts a lat/lon to an x/y in the RTMA grid
        args:
        :lat: float
        :lon: float
        returns: pair of tuples. First is x,y for RTMA grid, Second is snapped lat,lon for that point
        """
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

        return grid, closest

    def get_file_index(self):
        """
        Uses a list of valid RTMA points in txt file to determine how to find the archive containing that point
        return: index of x,y point
        """
        with open(self.VALID_COORDS, "r") as f:
            for i, line in enumerate(f):
                if line.strip() == str((self.x_grid, self.y_grid)):
                    return i

    def find_archive(self, index):
        """
        Uses a list of archives and an index to determine which archive the x,y point belongs in
        args:
        :index: from get_file_index
        return: archive containing data
        """
        with open(self.CHUNKS) as f:
            balls = [line.strip() for line in list(f)]
        ball_index = index // self.CHUNK_SIZE
        return balls[ball_index]


class SimpleGriddedDataset(GriddedDataset):
    """
    Abstract class for all other gridded datasets
    """
    SIG_DIGITS = 3

    @property
    def zero_padding(self):
        """
        Constant for how file names are formatted
        """
        return None

    def get_file_names(self):
        """
        Uses formatting and lat,lon to determine file name containing data
        return: dict with names for tar and gz versions of file
        """
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
        """
        General method for gridded datasets getting data
        args:
        :lat: float of latitude from which to get data
        :lon: float of longitude from which to get data
        return: tuple of lat/lon snapped to dataset grid, and weather data, is pd.Series with datetime or date index
        and str values corresponding to weather observations
        """
        super().get_data()
        first_metadata = self.get_metadata(self.head)
        if "cpcc" in self.dataset or "era5" in self.dataset:
            lat, lon = conventional_lat_lon_to_cpc(float(lat), float(lon))
        self.snapped_lat, self.snapped_lon = self.snap_to_grid(float(lat), float(lon), first_metadata)
        self.tar_name = self.get_file_names()["tar"]
        self.gzip_name = self.get_file_names()["gz"]
        ret_dict = {}
        for i, h in enumerate(self.get_hashes()):
            date_range = self.get_date_range_from_metadata(h)
            weather_dict = self.get_weather_dict(date_range, h, i == 0)
            ret_dict = {**ret_dict, **weather_dict}
        ret_lat, ret_lon = cpc_lat_lon_to_conventional(self.snapped_lat, self.snapped_lon)
        return (float(ret_lat), float(ret_lon)), pd.Series(ret_dict)

class Era5LandWind(SimpleGriddedDataset):
    """
    Abstract class from which ERA5 Land Wind datasets inherit. Sets formatting that these datasets use
    """
    @property
    def zero_padding(self):
        return 8

class StationDataset(IpfsDataset):
    """
    Instantiable class used for pulling in "ghcnd" or "ghcnd-imputed-daily" station data
    """
    @property
    def dataset(self):
        return self._dataset

    def __init__(self, dataset, ipfs_timeout=None):
        super().__init__(ipfs_timeout=ipfs_timeout)
        self._dataset = dataset

    def get_data(self, station):
        super().get_data()
        file_name = f"{self.head}/{station}.csv.gz"
        with gzip.open(self.get_file_object(file_name)) as gz:
            return gz.read().decode('utf-8')

class YieldDatasets(IpfsDataset):
    """
    Instantiable class used for pulling in sco and rma transitional yield data
    """
    @property
    def dataset(self):
        return self._dataset

    def __init__(self, dataset, ipfs_timeout=None):
        if dataset not in {
            "sco-yearly",
            "rmasco_imputed-yearly",
            "rma_t_yield-single-value",
            "rma_t_yield_imputed-single-value"
        }:
            raise ValueError("Invalid yield dataset")
        super().__init__(ipfs_timeout=ipfs_timeout)
        self._dataset = dataset

    def get_data(self, commodity, state, county):
        super().get_data()
        if "imputed" in self.dataset:
            file_name = f"{self.head}/{state}-{county}.csv"
        else:
            file_name = f"{self.head}/{commodity}-{state}-{county}.csv"
        return self.get_file_object(file_name).read().decode("utf-8")

class AemoDataset(IpfsDataset):
    """
    Abstract class from which all AEMO datasets inherit
    """
    @property
    @abstractmethod
    def data_file_name(self):
        """
        Name for all data files for dataset
        """
        pass

    def get_date_range_from_metadata(self, h):
        """
        args:
        :h: hash for ipfs directory containing metadata
        return: list of [start_time, end_time]
        """
        metadata = self.get_metadata(h)
        str_dates = (metadata["date_range"][0], metadata["date_range"][1])
        return [datetime.datetime.fromisoformat(dt) for dt in str_dates]

    def get_data(self):
        super().get_data()
        hashes = self.traverse_ll(self.head)
        ret_dict = {}
        for h in hashes:
            date_range = self.get_date_range_from_metadata(h)
            new_dict = self.extract_data_from_gz(date_range, h)
            ret_dict = {**ret_dict, **new_dict}
        return ret_dict

class AemoPowerDataset(AemoDataset):
    """
    Instantiable class for AEMO Victoria power data
    """
    @property
    def dataset(self):
        return "aemo-semihourly"

    @property
    def data_file_name(self):
        return "aeomo_update.gz"

    def extract_data_from_gz(self, date_range, ipfs_hash):
        with gzip.open(self.get_file_object(f"{ipfs_hash}/{self.data_file_name}")) as gz:
            cell_text = gz.read().decode('utf-8')
        time_itr = date_range[0]
        data_dict = {}
        for year_data in cell_text.split('\n'):
            for half_hour_data in year_data.split(','):
                if not half_hour_data:
                    demand, price = -9999, -9999
                else:
                    demand, price = half_hour_data.split("_")
                data_dict[time_itr] = {"demand": float(demand), "price": float(price)}
                time_itr = time_itr + datetime.timedelta(minutes=30)
        return data_dict


class AemoGasDataset(AemoDataset):
    """
    Instantiable class for AEMO Victoria gas data
    """
    @property
    def dataset(self):
        return "edd-daily"

    @property
    def data_file_name(self):
        return "edd_update.gz"

    def extract_data_from_gz(self, date_range, ipfs_hash):
        with gzip.open(self.get_file_object(f"{ipfs_hash}/{self.data_file_name}")) as gz:
            cell_text = gz.read().decode('utf-8')
        date_itr = date_range[0].date()
        data_dict = {}
        for year_data in cell_text.split('\n'):
            for day_data in year_data.split(','):
                data_dict[date_itr] = float(day_data)
                date_itr = date_itr + datetime.timedelta(days=1)
        return data_dict
