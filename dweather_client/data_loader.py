from dweather_client.http_client import get_rainfall_dict, get_temperature_dict, get_station_csv, parse_station_temps_as_dict
import dweather_client.ipfs_datasets

class GridCellDataLoader:

    _instances = {}

    def __init__(self, dataset, lat, lon, weather_type):
        """
        Args:
            lat (float): the grid cell latitude
            lon (float): the grid cell longitude
            weather_type (str): either rainfall or temperature
        """
        self.revisions = {}
        self.dataset = dataset
        self.lat = lat
        self.lon = lon
        assert weather_type == "rainfall" or weather_type == "temperature", \
            "weather_type must be 'rainfall' or 'temperature'"
        self.weather_type = weather_type

    def __new__(cls, dataset, lat, lon, weather_type):
        """ Create new class only if one with args doesn't exist
        Similar to singleton class, we only want one instance of DataLoader for each grid cell
        """
        if (dataset, lat, lon, weather_type) not in cls._instances:
            cls._instances[(dataset, lat, lon, weather_type)] = super(GridCellDataLoader, cls).__new__(cls)
        return cls._instances[(dataset, lat, lon, weather_type)]

    
    def load_revision(self, revision):
        """ Get the revision from ipfs and save it
        Args:
            revision (str): the named dataset revision on the ipfs gateway
        """
        if self.weather_type == "rainfall":
            metadata, data = get_rainfall_dict(self.lat, self.lon, revision, return_metadata=True)
        elif self.weather_type == "temperature":
            metadata, highs, lows = get_temperature_dict(self.lat, self.lon, revision, return_metadata=True)
            data = {"highs": highs, "lows": lows}
        self.revisions[revision] = {"metadata": metadata, "data": data}

    def populate_revision(self, revision):
        """ Fetch the revision only if we don't already have it
        Args:
            revision (str): the revision name on the ipfs gateway
        """
        if revision not in self.revisions:
            self.load_revision(revision)

    def get_revision(self, revision):
        """ Return the saved data revision if it exists, otherwise load it and return it
        Args:
            revisiont (str): the dataset revision name as on the ipfs gateway
        Returns:
            dictionary (str):
                {
                    "metadata": the revision metadata
                    "data": the revision data
                }
            for rainfall, "data" is a dict of datetime.date: rainfall values
            for temperature, "data" is a dict { "highs": dict, "lows": dict} with highs and lows associating datetime.dates with temps
        """
        self.populate_revision(revision)
        return self.revisions[revision]

    def reload(self):
        """ Get the lastest data from all saved revisions. Writes over saved revisions """
        for revision in self.revisions:
            self.load_revision(revision)

    def load_all_revisions(self):
        """ Get all revisions of the dataset. Writes over saved revisions"""
        for revision in dweather_client.ipfs_datasets.datasets[self.dataset]:
            self.load_revision(revision)

    def build_multi_revision_dict(self):
        """ Build out a dictionary using the most accurate data possible 
        Returns:
            dictionary
        """
        if self.weather_type == "rainfall":
            all_data_dict = {}
        elif self.weather_type == "temperature":
            all_data_dict = {
                "highs": {},
                "lows": {}
            }
        for revision in dweather_client.ipfs_datasets.datasets[self.dataset]:
            self.populate_revision(revision)
            if self.weather_type == "rainfall":
                new_data = {date: (self.revisions[revision]['data'][date], revision) for date in self.revisions[revision]["data"] if date not in all_data_dict}
                all_data_dict.update(new_data)
            elif self.weather_type == "temperature":
                new_highs = {date: (self.revisions[revision]['data']['highs'][date], revision) for date in self.revisions[revision]["data"]["highs"] if date not in all_data_dict["highs"]}
                new_lows = {date: (self.revisions[revision]['data']['lows'][date], revision) for date in self.revisions[revision]["data"]["lows"] if date not in all_data_dict["lows"]}
                all_data_dict["highs"].update(new_highs)
                all_data_dict["lows"].update(new_lows)

        return all_data_dict

class StationDataLoader:
    _instances = {}

    def __init__(self, station_id):
        """
        Args:
            station_id (str): the station id
        """
        self.station_id = station_id
        self.csv_text = ""

    def __new__(cls, station_id):
        """ Create new class only if one with args doesn't exist
        Similar to singleton class, we only want one instance of DataLoader for each station
        """
        if station_id not in cls._instances:
            cls._instances[station_id] = super(StationDataLoader, cls).__new__(cls)
        return cls._instances[station_id]

    def populate_csv(self):
        """ Get the revision from ipfs and save it
        Args:
            revision (str): the named dataset revision on the ipfs gateway
        """
        if self.csv_text == '':
            self.csv_text = get_station_csv(self.station_id)

    def get_temperatures(self, use_fahrenheit=True):
        """ Return the station data Tmins and Tmaxs 
        Args:
            use_fahrenheit: use degrees F if true, degrees C if false
        returns:
            tuple of dicts:
                datetime.date: float the daily highs
                datetime.date: float the daily lows
        """
        self.populate_csv()
        tmaxs, tmins = parse_station_temps_as_dict(self.csv_text, use_fahrenheit)
        return {"highs": tmaxs, "lows": tmins}
        

