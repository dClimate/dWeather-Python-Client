from dweather_client import utils, http_client
import os, io, requests, gzip
'''
def test_lat_lon_to_grid():
    heads = http_client.get_heads()
    rtma_hash = heads['rtma_pcp-hourly']
    r = requests.get('https://gateway.arbolmarket.com/ipfs/%s/grid_history.txt.gz' % rtma_hash)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as grid_history_file:
        grid_history = grid_history_file.read().decode('utf-8')
    assert utils.lat_lon_to_rtma_grid('40.752907470419586', '247.66162774628384', grid_history) == {'2011-01-01T00:00:00': ((491, 841), (491, 841)), '2016-01-06T14:00:00': (None, None)}
    assert utils.lat_lon_to_rtma_grid('20.191999000000006', '238.445999', grid_history) == {'2011-01-01T00:00:00': ((0 ,0), (0, 0)), '2016-01-06T14:00:00': ((0, 0), (0, 0))}


def test_rtma_grid_to_lat_lon():
    heads = http_client.get_heads()
    rtma_hash = heads['rtma_pcp-hourly']
    r = requests.get('https://gateway.arbolmarket.com/ipfs/%s/grid_history.txt.gz' % rtma_hash)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as grid_history_file:
        grid_history = grid_history_file.read().decode('utf-8')

        # case where lat/lon are the same
        assert utils.rtma_grid_to_lat_lon(0, 0, grid_history) == [('20.191999000000006', '238.445999'), ('20.191999000000006', '238.445999')]

        # random cases where lat/lon are different
        assert utils.rtma_grid_to_lat_lon(50, 54, grid_history) == [('21.61726877222153', '239.39106426923487'), ('21.617275250933048', '239.39106861956924')]
        assert utils.rtma_grid_to_lat_lon(130, 42, grid_history) == [('21.677552644312303', '241.3744282380296'), ('21.67755927656665', '241.37444172371673')]
        assert utils.rtma_grid_to_lat_lon(491, 841, grid_history) == [('40.752907470419586', '247.66162774628384'), ('40.75299702642884', '247.66167780662005')]
'''
def test_rtma_lookup():
    heads = http_client.get_heads()
    rtma_hash = heads['rtma_pcp-hourly']
    r = requests.get('https://gateway.arbolmarket.com/ipfs/%s/grid_history.txt.gz' % rtma_hash)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as grid_history_file:
        grid_history = grid_history_file.read().decode('utf-8')
    lookup = utils.build_rtma_lookup(grid_history)
    reverse_lookup = utils.build_rtma_reverse_lookup(grid_history)
    for rev_lookup_lon in reverse_lookup['2016-01-06T14:00:00']['lon']:
        rev_lookup_x, rev_lookup_y = reverse_lookup['2016-01-06T14:00:00']['lon'][rev_lookup_lon]
        assert (rev_lookup_x, rev_lookup_y) == reverse_lookup['2016-01-06T14:00:00']['lon'][rev_lookup_lon]
        assert lookup['2016-01-06T14:00:00'][1][rev_lookup_y][rev_lookup_x] == rev_lookup_lon

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

def test_get_n_closest_station_ids():
    heads = http_client.get_heads()
    ghcnd_metadata = http_client.get_metadata(heads['ghcnd'])

    # get the 20 closest station ids to a spot in Kentucky
    ids = utils.get_n_closest_station_ids(37, -85, ghcnd_metadata, 20)
    print(ids)

def test_cpc_lat_lon_to_conventional():
    # case where coords are ok:
    lat = 25.000
    lon = 45.000
    new_lat, new_lon = utils.cpc_lat_lon_to_conventional(lat, lon)
    assert new_lat == lat
    assert new_lon == lon
    # case where lon needs to be converted
    lat = 25.000
    lon = 262.000
    new_lat, new_lon = utils.cpc_lat_lon_to_conventional(lat, lon)
    assert new_lat == lat
    assert new_lon == -98.000
    

def test_conventional_lat_lon_to_cpc():
    # case where coords are ok:
    lat = 25.000
    lon = 45.000
    new_lat, new_lon = utils.conventional_lat_lon_to_cpc(lat, lon)
    assert new_lat == lat
    assert new_lon == lon
    # case where lon needs to be converted
    lat = 25.000
    lon = -98.000
    new_lat, new_lon = utils.conventional_lat_lon_to_cpc(lat, lon)
    assert new_lat == lat
    assert new_lon == 262.000
    
