from dweather_client.df_arithmetic import cat_icao_stations, cat_n_closest_station_dfs
from dweather_client.http_client import get_heads, get_metadata


def test_cat_n_closest_station_dfs():
    ghcndi_hash = get_heads()["ghcnd-imputed-daily"] 
    stations = cat_n_closest_station_dfs( \
        36, 
        -94.5, 
        10, 
        pin=False, 
        force_hash=ghcndi_hash
    )
