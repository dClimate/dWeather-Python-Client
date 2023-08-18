import pandas as pd
import zipfile
import os
import re
from io import StringIO

# Path to the folder containing the station CSV files
station_folder = '/Users/maana/Documents/GitHub/dWeather-Python-Client/Station'

# Path to the input zip file
input_zip_file = '/Users/maana/Documents/GitHub/dWeather-Python-Client/cme_futures_hist_by_station.zip'

# Output folder for merged data
output_folder = 'CME_DDIF'

# Create a dictionary to store the station data
station_data = {}

# Process the station CSV files
for root, dirs, files in os.walk(station_folder):
    for file in files:
        if file.endswith('.csv'):
            station_name = os.path.splitext(file)[0]
            file_path = os.path.join(root, file)

            # Read the station CSV file and store the data in the dictionary
            station_data[station_name] = pd.read_csv(file_path)

# Process the zip files
with zipfile.ZipFile(input_zip_file, 'r') as input_zip_ref:
    file_list = input_zip_ref.namelist()
    with open('station_data.txt', 'w', encoding='ascii') as station_data_ref:
        keys = station_data.keys()
        for key in keys:
            station_data_ref.write('!' + key + '!' + "\n")

    for file_name in file_list:
        # Check if the file is a CSV file
        if not file_name.endswith('.csv'):
            continue

        # Extract the station name from the file name
        station_name = re.match(r'(.+)/data.csv', file_name).group(1)
        with open('station_name.txt', 'a', encoding='ascii') as station_name_ref:
            station_name_ref.write('!' + station_name + '!' + "\n")
        if station_name is None:
            continue

        # Check if the station has corresponding data in the station data dictionary
        if station_name in station_data:
            # Read the CSV data from the zip file
            with input_zip_ref.open(file_name) as input_csv_file_ref:
                binary_data = input_csv_file_ref.read()
                ascii_data = binary_data.decode('ascii')
                zip_data = pd.read_csv(StringIO(ascii_data))

            # Merge the station data with the zip data
            merged_data = pd.concat([station_data[station_name], zip_data], ignore_index=True)
            merged_data.drop_duplicates(subset=['dt', 'SETT'], keep='last', inplace=True)

            # Save the merged data as a new CSV file for the station
            os.makedirs(output_folder, exist_ok=True)
            output_file_name = f"{station_name}.csv"
            output_csv_file = os.path.join(output_folder, output_file_name)
            merged_data.to_csv(output_csv_file, index=False)

            # Remove the station entry from the station data dictionary to keep track of processed stations
            del station_data[station_name]
        else:
            # Read the CSV data from the zip file
            with input_zip_ref.open(file_name) as input_csv_file_ref:
                binary_data = input_csv_file_ref.read()
                ascii_data = binary_data.decode('ascii')
                zip_data = pd.read_csv(StringIO(ascii_data))

            # Save the CSV data as a new CSV file in the output folder
            os.makedirs(output_folder, exist_ok=True)
            output_file_name = f"{station_name}.csv"
            output_csv_file = os.path.join(output_folder, output_file_name)
            zip_data.to_csv(output_csv_file, index=False)

    # Add the files from the "station" folder that are not in "cme_futures_hist_by_station" to the "CME_DDIF" folder
    for station_name in station_data.keys():
        station_csv_data = station_data[station_name]
        output_file_name = f"{station_name}.csv"
        output_csv_file = os.path.join(output_folder, output_file_name)
        station_csv_data.to_csv(output_csv_file, index=False)

