from dweather_client.df_arithmetic import cat_icao_stations, cat_n_closest_station_dfs
from dweather_client.http_client import get_heads, get_metadata

#def test_cat_icao_stations():
#    ghcnd_hash = get_heads()['ghcnd']
#    stations = cat_icao_stations(pin=False, force_hash=ghcnd_hash)

def test_cat_n_closest_station_dfs():
    ghcnd_hash = get_heads()['ghcnd'] 
    stations = cat_n_closest_station_dfs( \
        36, 
        -94.5, 
        10, 
        pin=False, 
        force_hash=ghcnd_hash
    )
