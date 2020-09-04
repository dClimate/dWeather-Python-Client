# dWeather-client-python


virtualenv .
bin/pip3 install -r requirements.txt
bin/python3 -m 



### IPFS.py (IPFS pandas dataframe loader)
Sample Calls:

`import IPFS`

Get rainfall data:
```
a = IPFS.IPFSAnalysis()

a.rainfall_single_loc(loc, dataset, by='', startDate=False, endDate=False, byID=False, byInches=True)

a.df
```

Get station data multi location:
```
a = IPFS.IPFSAnalysis()

df = ghcnd_multi_station_csv(["STATIONID1", "STATIONIDN"])

print(df)
```

Some functions for grabbing data from Arbol's IPFS nodes

### A word about how the data is structured

The data are hosted on Arbol's ipfs node and accessed through an ipfs hash. The data retrieval functions can be used without needing to know how the ipfs node is structured, but included in this library are some functions that allow deeper inspection of the Node. Feel free to skip this section if all you need is the data.

There are two nodes Arbol uses for hosting data, a production node and a development node. These nodes have fixed urls (ipfs.arbolmarket.com and ipfs-c.arbolmarket.com), but at any given time they are liable to switch which address is for prod and which one is for dev. In order to know the active server a textfile is hosted at at https://ipfs.arbolmarket.com/climate/active-server which can be read to view the current production server. 

Once the server is known, the hash of the proper dataset must be obtained. A list of hashes can be found at https://{active-sever}/climate/hashes.json and is a json-parsible dictionary of datasets and their hashes. 

Most datasets are gridded, and each gridded dataset may have different qualities that should be known (resolution, units of measurement, time of last update, etc). This file is located on ipfs at https://{active-sever}/ipfs/{hash}/metadata.json. With the knowledge of the metadata in hand, the data for an individual grid cell can be read at https://{active-sever}/ipfs/{hash}/{lat}\_{lon}

The only dataset currently on ipfs that is not gridded is ghcnd, individual gzipped csv files for a station can be found at https://{active-sever}/ipfs/{hash}/{station_id}.csv.gz

### key functions

##### Give me the data already

`get_rainfall_as_dict(lat, lon, dataset)`

Returns the entire weather history as a dicitonary with datetime.date keys and rainfall (in mm) values

`get_temperatures_as_dict(lat, lon, dataset)`

Returns the entire weather history as a dicitonary with datetime.date keys and degrees values

`get_station_data_csv(station_id)`

Returns the csv file `<station_id>.csv` in its entirety as a string

##### advanced data retrieval

These methods are used to get common sums

`get_rainfall_over_period(lat, lon, dataset, start_date, end_date, daily_cap=None)`

Similar to the `get_rainfall_as_dict()` method, except now returns the total cumulative rainfall between two `datetime.date`s. Optional argument daily_cap limits how much any one day of rainfall can contribute. The return value is a tuple of size 2, the first item being the total cumulative rainfall, the second being the number of days in the period (useful for computing an average rainfall per day)

`get_yearly_term_rainfall(lat, lon, dataset, start_date, end_date, daily_cap=None, start_year=HISTORICAL_START_YEAR, end_year=None)`

This method behaves extremely similarly to the `get_rainfall_over_period()` method, but it performs the same term rainfall accumulation for each year between and including the optional start_year and end_year arguments. If omitted, start year will be 1981, and `end_year` will be the year of the `start_date` term. The return value is a dictionary, where the keys are years and the values are the same tuple that `get_rainfall_over_period` uses. As an example, if you wanted to view the cumulative rainfall over every March from 1990 to 2000, you would call `get_yearly_term_rainfall(<lat>, <lon>, <dataset>, datetime.date(2020, 3, 1), datetime.date(2020, 3, 31), start_year=1990, end_year=2000)`

##### Get data about the ipfs node

`getMetadata(url, hash_str)`

Returns the metadata json on the server `url` for the dataset at the hash `hash_str`. Will raise an error if the metadata is not there.

`getIPFSHashes(url)`

Returns the contents of hashes.json for th server at `url`. Will raise an error if the hashes.json file is not present on the sever.

`getActiveServer()`

Returns the contetns of the `active-server` file

##### Utilities

`list_from_date_range(start_date, end_date)`

Returns a list of datetime.dates between and including start_date and end_date

`getZippedCoordinateText(url, hash_str, coord_str)`

Downloads and unzips the grid cell file on the server `url` with the dataset hash `hash_str` and the coordiantes `coord_str`

`getCoordinateText(url, hash_str, coord_str)`

Same as `getZippedCoordinateText` but for files that aren't using gzip compression.

### legacy stuff

A few projects may still be using these functions but they have been superceded by the other data retreival functions

`sumRainfallOverDailyPeriod`
`sum_rain_all_years`
`lookup_historical_rainfall`
`lookup_historical_temps`
