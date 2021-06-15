from dweather_client.grid_utils import *
from dweather_client.http_queries import get_heads, get_metadata

def test_grid():
    get_n_closest_station_ids(38, -94, 5)

def test_cpc_lat_lon_to_conventional():
    # case where coords are ok:
    lat = 25.000
    lon = 45.000
    new_lat, new_lon = cpc_lat_lon_to_conventional(lat, lon)
    assert new_lat == lat
    assert new_lon == lon
    # case where lon needs to be converted
    lat = 25.000
    lon = 262.000
    new_lat, new_lon = cpc_lat_lon_to_conventional(lat, lon)
    assert new_lat == lat
    assert new_lon == -98.000

def test_conventional_lat_lon_to_cpc():
    # case where coords are ok:
    lat = 25.000
    lon = 45.000
    new_lat, new_lon = conventional_lat_lon_to_cpc(lat, lon)
    assert new_lat == lat
    assert new_lon == lon
    # case where lon needs to be converted
    lat = 25.000
    lon = -98.000
    new_lat, new_lon = conventional_lat_lon_to_cpc(lat, lon)
    assert new_lat == lat
    assert new_lon == 262.000    