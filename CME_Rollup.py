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
    cme_futures_obj = StationForecastDataset("cme_futures-daily", ipfs_timeout=None)
    current_head = cme_futures_obj.head
    current_metadata = cme_futures_obj.get_metadata(current_head)
    station_data_dictionary = {} # Key will be the station, and the value will be a pandas dataframe with the data
    dataframes = {}
    while True: 
        current_datetime = datetime.datetime.strptime(current_metadata["date range"][0], "%Y-%m-%d").date()
        station_features = json.loads(cme_futures_obj.get_stations(current_datetime))['features']
        for feature in station_features: 
            station_name = feature["properties"]["station name"] #this is the station name     
            csv_text = cme_futures_obj.get_data(station_name, (current_datetime))
            df = pd.read_csv(StringIO(csv_text))
            try:
                df.rename(columns={"DATE": "forecasted_dt", "value": "SETT"}, inplace=True)
                df["dt"] = current_datetime.strftime("%Y-%m-%d")
            except KeyError:
                print("Required columns not found in the dataset. Please verify the column names.")
                continue
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


    for station_key, data in station_data_dictionary.items():
        station_filename = f"{station_key}.csv"  # Generate a filename based on the station name
        with open(station_filename, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["forecasted_dt", "SETT", "dt"])
            for hash_value, previous_hash, forecast_date in zip(data["hashes"], data["previous_hashes"], data["forecast_dates"]):
                df = dataframes[station_key][hash_value]
                for _, row in df.iterrows():
                    forecasted_dt = str(row['forecasted_dt'])
                    forecasted_dt = forecasted_dt[:-3]
                    sett = str(row['SETT'])
                    dt = str(forecast_date)
                    writer.writerow([forecasted_dt, sett, dt])
    station_data = {}
    for file_station_key, data in station_data_dictionary.items():
        station_filename = f"{file_station_key}.csv"  # Generate the filename of the CSV file
        file_path = os.path.join(data_directory, station_filename)  # Create the full file path
        station_data[file_station_key] = pd.read_csv(file_path)

    """file_station_name = "D2X"
    station_df = station_data[file_station_name]
    station_df['dt'] = pd.to_datetime(station_df['dt']).dt.strftime("%Y-%m-%d")
    print(station_df)"""
    import ipdb;ipdb.set_trace()


    

