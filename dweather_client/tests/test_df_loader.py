from dweather_client import df_loader, utils
import datetime

def test_rainfall_df():
    rainfall_df = df_loader.get_rainfall_df(41.125, -75.125, 'chirps_05-daily')

def test_temperature_df():
	temp_df = df_loader.get_temperature_df(43.083, -92.000, 'prism_temp-daily')
	jan_2020 = temp_df.loc[datetime.date(2020, 1, 1)]
	assert(jan_2020["HIGH"] == 25.26)
	assert(jan_2020["LOW"] == 12.22)

def test_station_dfs():
    station_df = df_loader.get_station_rainfall_df('USW00024285')
    station_df2 = df_loader.get_station_temperature_df('USW00024285')
    station_df3 = df_loader.get_station_snow_df('USW00024285')
