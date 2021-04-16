from dweather_client.grid_utils import *
from dweather_client.http_queries import get_heads, get_metadata

def test_grid():
    get_n_closest_station_ids(38, -94, 5)

def test_snap_to_grid_chirps():
    heads = get_heads()
    chirps_metadata = get_metadata(heads['chirps_05-daily'])
    lat, lon = snap_to_grid(41.1842, -75.11, chirps_metadata)
    assert lat == 41.175
    assert lon == -75.125

def test_snap_to_grid_prism():
    heads = get_heads()
    prism_metadata = get_metadata(heads['prism_precip-daily'])
    lat, lon = snap_to_grid(39.8398, -104.193, prism_metadata)
    assert lat == 39.833
    assert lon == -104.208

def test_snap_to_grid_cpc_global_daily():
    # tests snap_to_grid with ideosyncratic cpc lat/lon format
    heads = get_heads()
    prism_metadata = get_metadata(heads['cpc_global-daily'])
    lat, lon = snap_to_grid(69.754, 330.759, prism_metadata)
    assert lat == 69.750
    assert lon == 330.750

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