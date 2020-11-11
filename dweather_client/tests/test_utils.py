from dweather_client import utils, http_client

def test_snap_to_grid_chirps():
    heads = http_client.get_heads()
    chirps_metadata = http_client.get_metadata(heads['chirps_05-daily'])
    lat, lon = utils.snap_to_grid(41.1842, -75.11, chirps_metadata)
    assert lat == 41.175
    assert lon == -75.125

def test_snap_to_grid_prism():
    heads = http_client.get_heads()
    prism_metadata = http_client.get_metadata(heads['prism_precip-daily'])
    lat, lon = utils.snap_to_grid(39.8398, -104.193, prism_metadata)
    assert lat == 39.833
    assert lon == -104.208

def test_snap_to_grid_cpc_global_daily():
    # tests snap_to_grid with ideosyncratic cpc lat/lon format
    heads = http_client.get_heads()
    prism_metadata = http_client.get_metadata(heads['cpc_global-daily'])
    lat, lon = utils.snap_to_grid(69.754, 330.759, prism_metadata)
    assert lat == 69.750
    assert lon == 330.750
