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

