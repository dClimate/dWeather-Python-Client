from dweather_client import df_utils

def test_icao():
    ids = df_utils.get_station_ids_with_icao()
