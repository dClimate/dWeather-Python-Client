from dweather_client import df_loader, utils
import datetime

def test_rainfall_df():
    rainfall_df = df_loader.get_rainfall_df(41.125, -75.125, 'chirps_05-daily')

def test_temperature_df():
    temp_df = df_loader.get_temperature_df(43.083, -92.000, 'prism_temp-daily')

def test_station_dfs():
    station_df = df_loader.get_station_rainfall_df('USW00024285')
    station_df2 = df_loader.get_station_temperature_df('USW00024285')
    station_df3 = df_loader.get_station_snow_df('USW00024285')

def test_get_simulated_hurricane_df():
    df_all_ni = df_loader.get_simulated_hurricane_df('NI')
    df_subset_circle_ni = df_loader.get_simulated_hurricane_df('NI', radius=500, lat= 21, lon=65)
    df_subset_box_ni = df_loader.get_simulated_hurricane_df('NI', min_lat= 21, max_lat=22, min_lon=65, max_lon=66)
    
    assert len(df_all_ni.columns) == len(df_subset_circle_ni.columns) == len(df_subset_box_ni.columns) == 10
    assert len(df_subset_circle_ni) < len(df_all_ni)
    assert len(df_subset_box_ni) < len(df_all_ni)

def test_get_historical_hurricane_df():
    df_all_na = df_loader.get_historical_hurricane_df('NA')
    df_subset_circle_na = df_loader.get_historical_hurricane_df('NA', radius=50, lat=26, lon=-90)
    df_subset_box_na = df_loader.get_historical_hurricane_df('NA', min_lat=26, max_lat=26.5, min_lon=-91, max_lon=-90.5)

    assert len(df_all_na.columns) == len(df_subset_circle_na.columns) == len(df_subset_box_na.columns) == 163
    assert len(df_subset_circle_na) < len(df_all_na)
    assert len(df_subset_box_na) < len(df_all_na)

def test_get_atcf_hurricane_df():
    df_all_al = df_loader.get_atcf_hurricane_df('AL')
    df_subset_circle_al = df_loader.get_atcf_hurricane_df('AL', radius=50, lat=26, lon=-90)
    df_subset_box_al = df_loader.get_atcf_hurricane_df('AL', min_lat=26, max_lat=26.5, min_lon=-91, max_lon=-90.5)

    assert len(df_all_al.columns) == len(df_subset_circle_al.columns) == len(df_subset_box_al.columns) == 37
    assert len(df_subset_circle_al) < len(df_all_al)
    assert len(df_subset_box_al) < len(df_all_al)

def test_get_era5_df():
    lat, lon = 35.70284883765463, -81.29880863239713
    df = df_loader.get_era5_df(lat, lon, 'era5_land_wind_u-hourly')[1]
    first_time, last_time = df.index[0], df.index[-1]
    assert (last_time - first_time).days * 24 == len(df) - 1
