from dweather_client.ipfs_errors import *
from dweather_client.ipfs_queries import * 
from dweather_client.tests.mock_fixtures import get_patched_datasets
from dweather_client.client import get_australia_station_history, get_station_history, get_gridcell_history, get_tropical_storms,\
    get_yield_history, get_irrigation_data, get_power_history, get_gas_history, get_alberta_power_history, GRIDDED_DATASETS, has_dataset_updated,\
    get_forecast_datasets, get_forecast, get_cme_station_history, get_european_station_history, get_hourly_station_history, get_drought_monitor_history, get_japan_station_history,\
    get_afr_history, get_cwv_station_history, get_teleconnections_history, get_station_forecast_history, get_station_forecast_stations, get_eaufrance_history
from dweather_client.aliases_and_units import snotel_to_ghcnd
import pandas as pd
from io import StringIO
import datetime
from astropy import units as u
from astropy.units import imperial
import pytest
import os
import csv
data_directory = os.getcwd()

if __name__ == '__main__':
    #cme_futures_obj = StationForecastDataset("cme_futures-daily", ipfs_timeout=ipfs_timeout)
    cme_futures_obj = StationForecastDataset("cme_futures-daily", ipfs_timeout=None)
    current_head = cme_futures_obj.head
    current_metadata = cme_futures_obj.get_metadata(current_head)
    station_data_dictionary = {} # Key will be the station, and the value will be a pandas dataframe with the data
    dataframes = {}
    while True: #current_metadata['previous hash'] is not None:
        #current_date = current_metadata["date range"][0]
        #current_datetime = datetime.date(int(current_date[0:4]), int(current_date[5:7]), int(current_date[8:])) #Beware current date's type changes here
        current_datetime = datetime.datetime.strptime(current_metadata["date range"][0], "%Y-%m-%d").date()
        station_features = json.loads(cme_futures_obj.get_stations(current_datetime))['features']
        for feature in station_features: 
            station_name = feature["properties"]["station name"] #this is the station name
            
            csv_text = cme_futures_obj.get_data(station_name, (current_datetime))
            df = pd.read_csv(StringIO(csv_text))
            df['forecast_date'] = current_datetime
            if station_name not in station_data_dictionary:
                station_data_dictionary[station_name] = {
                    "hashes": [current_head],
                    "previous_hashes": [],
                    "forecast_dates": [current_datetime.strftime("%Y-%m-%d")],
                }
            else:
                station_data_dictionary[station_name]["hashes"].append(current_head)
                station_data_dictionary[station_name]["forecast_dates"].append(current_datetime.strftime("%Y-%m-%d"))

            if station_name not in dataframes:
                dataframes[station_name] = {}
            dataframes[station_name][current_head] = df

        if current_metadata["previous hash"] is None:
            break

        previous_hash = current_metadata["previous hash"]
        current_head = previous_hash
        current_metadata = cme_futures_obj.get_metadata(current_head)

    for station_key, data in station_data_dictionary.items():
        data["previous_hashes"] = data["hashes"][1:]
        data["hashes"] = data["hashes"][:-1]

    # Print the station data dictionary in the desired format
    for station_key, data in station_data_dictionary.items():
        station_filename = f"{station_key}_table.csv"  # Generate a filename based on the station name
        with open(station_filename, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["DATE", "SETT", "forecast_date"])
            for hash_value, previous_hash, forecast_date in zip(data["hashes"], data["previous_hashes"], data["forecast_dates"]):
                df = dataframes[station_key][hash_value]
                for _, row in df.iterrows():
                    date = str(row['DATE'])
                    sett = str(row['SETT'])
                    forecast_date = str(forecast_date)
                    writer.writerow([date, sett, forecast_date])
    station_data = {}
    for file_station_key, data in station_data_dictionary.items():
        station_filename = f"{file_station_key}_table.csv"  # Generate the filename of the CSV file
        file_path = os.path.join(data_directory, station_filename)  # Create the full file path
        station_data[file_station_key] = pd.read_csv(file_path)


    import ipdb;ipdb.set_trace()

    