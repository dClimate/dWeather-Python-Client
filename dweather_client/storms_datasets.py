import gzip
import json
import datetime
from abc import abstractmethod

import pandas as pd

from dweather_client.df_utils import boxed_storms, nearby_storms
from dweather_client.ipfs_queries import IpfsDataset

def process_df(input_df, **kwargs):
    if {'radius', 'lat', 'lon'}.issubset(kwargs.keys()):
        df = nearby_storms(input_df, kwargs['lat'], kwargs['lon'], kwargs['radius'])
    elif {'min_lat', 'min_lon', 'max_lat', 'max_lon'}.issubset(kwargs.keys()):
        df = boxed_storms(input_df, kwargs['min_lat'], kwargs['min_lon'], kwargs['max_lat'], kwargs["max_lon"])
    else:
        df = input_df
    return df
            
class IbtracsDataset(IpfsDataset):
    dataset = "ibtracs_storm_basins"

    def get_data(self, basin, **kwargs):
        if basin not in {'NI', 'SI', 'NA', 'EP', 'WP', 'SP', 'SA'}:
            raise ValueError("Invalid basin ID")
        super().get_data()
        ipfs_hash = self.get_relevant_hash(kwargs["as_of"])
        file_obj = self.get_file_object(f"{ipfs_hash}/ibtracs-{basin}.csv.gz")
        df = pd.read_csv(
            file_obj, na_values=["", " "], keep_default_na=False, low_memory=False, compression="gzip"
        )
        df = df[1:]
        df["lat"] = df.LAT.astype(float)
        df["lon"] = df.LON.astype(float)
        del df["LAT"]
        del df["LON"]

        processed_df = process_df(df, **kwargs)

        processed_df["HOUR"] = pd.to_datetime(processed_df["ISO_TIME"])
        del processed_df["ISO_TIME"]

        return processed_df

    def get_relevant_hash(self, as_of_date):
        """
        return the ipfs hash required to pull in data for a forecast date
        """
        cur_hash = self.head
        if as_of_date == None:
            return cur_hash
        cur_metadata = self.get_metadata(cur_hash)
        # This routine is agnostic to the order of data contained in the hashes (at a cost of inefficiency) -- if the data contains the forecast date, it WILL be found, eventually
        most_recent_date = datetime.datetime.fromisoformat(cur_metadata["time generated"]).date()
        if as_of_date >= most_recent_date:
            return cur_hash
        prev_hash = cur_metadata['previous hash']
        while prev_hash is not None:
            prev_metadata = self.get_metadata(prev_hash)
            prev_date = datetime.datetime.fromisoformat(prev_metadata["time generated"]).date()
            if prev_date <= as_of_date <= most_recent_date:
                return prev_hash
            # iterate backwards in the link list one step
            try:
                prev_hash = prev_metadata['previous hash']
                most_recent_date = prev_date
            except KeyError:
                # Because we added the as_of after a while to this ETL prev_hash won't be 'None' we'll just run into an exception
                raise DateOutOfRangeError(
                    "as_of data is earlier than earliest available hash")


class AtcfDataset(IpfsDataset):
    dataset = "atcf_btk-seasonal"

    def get_data(self, basin, **kwargs):
        if basin not in {'AL', 'CP', 'EP', 'SL'}:
            raise ValueError("Invalid basin ID")
        super().get_data()
        release_ll = self.traverse_ll(self.head)
        hurr_dict = {}
        for release_hash in release_ll:
            release_file = self.get_file_object(f"{release_hash}/history.json.gz")
            with gzip.open(release_file) as zip_data:
                release_content = json.load(zip_data)
            try:
                hurr_dict['features'] += release_content['features']
            except KeyError:
                hurr_dict.update(release_content)

        features = hurr_dict['features']
        df_list = []
        for feature in features:
            sub_dict = feature['properties']
            sub_dict['lat'] = feature['geometry']['coordinates'][0]
            sub_dict['lon'] = feature['geometry']['coordinates'][1]
            df_list.append(sub_dict)
        df = pd.DataFrame(df_list)
        df = df[df["BASIN"] == basin]
        df['HOUR'] = pd.to_datetime(df["HOUR"])

        processed_df = process_df(df, **kwargs)

        for col in processed_df:
            if col != "HOUR":
                processed_df[col] = pd.to_numeric(processed_df[col], errors='ignore')

        return processed_df

class SimulatedStormsDataset(IpfsDataset):
    dataset = "storm-simulated-hurricane"

    def get_data(self, basin, **kwargs):
        super().get_data()
        
        if basin not in {'EP', 'NA', 'NI', 'SI', 'SP', 'WP'}:
            raise ValueError("Invalid basin ID")

        metadata  = self.get_metadata(self.head)
        dfs = []
        for f in metadata["files"]:
            if basin in f:
                file_obj = self.get_file_object(f"{self.head}/{f}")
                df = pd.read_csv(file_obj, header=None, compression="gzip")[range(10)]
                columns = ['year', 'month', 'tc_num', 'time_step', 'basin', 'lat', 'lon', 'min_press', 'max_wind', 'rmw']
                df.columns = columns
                df["sim"] = f[-8]
                dfs.append(df)

        big_df = pd.concat(dfs).reset_index(drop=True)
        big_df.loc[big_df.lon > 180, 'lon'] = big_df.lon - 360

        return process_df(big_df, **kwargs)
