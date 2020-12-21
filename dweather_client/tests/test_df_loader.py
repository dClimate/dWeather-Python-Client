from dweather_client import df_loader, utils
import datetime

def test_rtma_df():
    client = df_loader.RTMADFClient()
    lat_lon, rtma_df = client.get_best_rtma_df('42.01', '-117.01')
    assert client.get_best_rtma_df(40.0001, -69.000001)[0] == (39.999662751557175, -68.9922083077804)
    assert client.get_best_rtma_df(40.99999, -69.000001)[0] == (41.00782433956041, -68.9996167647198)
    assert client.get_best_rtma_df(41, -69.000001)[0] == (41.00782433956041, -68.9996167647198)
    assert client.get_best_rtma_df(41, -71.99999)[0] == (40.99988297329153, -72.01208284896211)

def test_rainfall_df():
    rainfall_df = df_loader.get_rainfall_df(41.125, -75.125, 'chirps_05-daily')

def test_temperature_df():
    temp_df = df_loader.get_temperature_df(43.083, -92.000, 'prism_temp-daily')

def test_station_dfs():
    station_df = df_loader.get_station_rainfall_df('USW00024285')
    station_df2 = df_loader.get_station_temperature_df('USW00024285')
    station_df3 = df_loader.get_station_snow_df('USW00024285')
