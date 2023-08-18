import pandas as pd
import json
import os

# Load cme_futures.json file
with open('CME_Futures.json') as f:
    variables_data = json.load(f)

# Read cme_weather_specs.xlsx file
specs_data = pd.read_excel('cme_weather_specs.xlsx')
missing_stations_data = [
    {'Commodity Code': 'KRK', 'Contract Name': 'CME Seasonal Strip Degree Days Index Futures - Houston CDD May'},
    {'Commodity Code': 'K6', 'Contract Name': 'CME Degree Days Index Futures - Philadelphia CDD'},
    {'Commodity Code': 'KW', 'Contract Name': 'CME Degree Days Index Futures - Boston CDD'}
]
missing_data_df = pd.DataFrame(missing_stations_data)
specs_data = pd.concat([specs_data, missing_data_df], ignore_index=True)

# Generate stations.json and find the earliest and most recent dates
stations = {
    "type": "FeatureCollection",
    "features": []
}
earliest_date = None
most_recent_date = None

# Get a list of CSV files directly under the CME_DDIF folder
csv_files = [file for file in os.listdir('CME_DDIF') if file.endswith('.csv')]

for csv_file in csv_files:
    csv_file_path = os.path.join('CME_DDIF', csv_file)
    station_name = os.path.splitext(csv_file)[0]
    code = station_name  # Assuming station name corresponds to the code

    
    # Check if code exists in specs_data DataFrame
    
    
    # Check if code exists in specs_data DataFrame
    if code not in specs_data['Commodity Code'].values:
        continue
    
    # Retrieve the description from specs_data DataFrame
    description = specs_data.loc[specs_data['Commodity Code'] == code, 'Contract Name'].values[0]
    
    merged_data = pd.read_csv(csv_file_path)
    
    date_range = [
        str(merged_data['dt'].min()),
        str(merged_data['dt'].max())
    ]
    
    if earliest_date is None or merged_data['dt'].min() < earliest_date:
        earliest_date = merged_data['dt'].min()
    
    if most_recent_date is None or merged_data['dt'].max() > most_recent_date:
        most_recent_date = merged_data['dt'].max()

    station = {
        "type": "Feature",
        "properties": {
            "file name": csv_file,
            "station name": station_name,
            "description": [description],
            "variables": {
                "0:": {
                    "column name": variables_data["0"]["column name"],
                    "plain text description": variables_data["0"]["plain text description"],
                    "unit of measurement": variables_data["0"]["unit of measurement"],
                    "precision": variables_data["0"]["precision"],
                    "na value": variables_data["0"]["na value"],
                },
                "1:": {
                    "column name": variables_data["1"]["column name"],
                    "plain text description": variables_data["1"]["plain text description"],
                    "unit of measurement": variables_data["1"]["unit of measurement"],
                    "precision": variables_data["1"]["precision"],
                    "na value": variables_data["1"]["na value"]
                },
                "2:": {
                    "column name": variables_data["2"]["column name"],
                    "plain text description": variables_data["2"]["plain text description"],
                    "unit of measurement": variables_data["2"]["unit of measurement"],
                    "precision": variables_data["2"]["precision"],
                    "na value": variables_data["2"]["na value"]
                }
            },
            "date range": date_range
        }
    }
    stations["features"].append(station)

# Save stations.json
stations_file_path = os.path.join('CME_DDIF', 'stations.json')
with open(stations_file_path, 'w') as outfile:
    json.dump(stations, outfile, indent=4)

# Generate metadata.json
metadata = {
    "compression": None,
    "name": "cme_ddif",
    "documentation": "https://www.cmegroup.com/content/dam/cmegroup/rulebook/CME/IV/400/403/403.pdf",
    "description": "HDD, CDD, and CAT futures settlement data from the Chicago Mercantile Exchange (CME) by city",
    "publisher": "Chicago Mercantile Exchange",
    "source data url": "ftp.cmegroup.com",
    "tags": [
        "temperature",
        "Europe",
        "U.S",
        "CME"
    ],
    "date range": [
        str(earliest_date),
        str(most_recent_date)
    ],
    "station metadata": "stations.json",
    "previous hash": None,
    "time generated": "",
    "data dictionary": {
        "0:": {
            "column name": variables_data["0"]["column name"],
            "plain text description": variables_data["0"]["plain text description"],
            "unit of measurement": variables_data["0"]["unit of measurement"],
            "precision": variables_data["0"]["precision"],
            "na value": variables_data["0"]["na value"],
        },
        "1:": {
            "column name": variables_data["1"]["column name"],
            "plain text description": variables_data["1"]["plain text description"],
            "unit of measurement": variables_data["1"]["unit of measurement"],
            "precision": variables_data["1"]["precision"],
            "na value": variables_data["1"]["na value"]
        },
        "2:": {
            "column name": variables_data["2"]["column name"],
            "plain text description": variables_data["2"]["plain text description"],
            "unit of measurement": variables_data["2"]["unit of measurement"],
            "precision": variables_data["2"]["precision"],
            "na value": variables_data["2"]["na value"]
        }
    }
}

# Save metadata.json
metadata_file_path = os.path.join('CME_DDIF', 'metadata.json')
with open(metadata_file_path, 'w') as outfile:
    json.dump(metadata, outfile, indent=4)

