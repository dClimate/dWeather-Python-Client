import df_loader
import utils

def test_rainfall_df():
    rainfall_df = df_loader.get_rainfall_df(41.125, -75.125, 'chirps_05-daily')

def test_station_dfs():
    station_df = df_loader.get_station_rainfall_df('USW00024285')
    station_df2 = df_loader.get_station_temperature_df('USW00024285')
    station_df3 = df_loader.get_station_snow_df('USW00024285')