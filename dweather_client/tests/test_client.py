from dweather_client.client import get_station_history, get_gridcell_history, get_tropical_storms,\
    get_yield_history, get_power_history, get_gas_history, GRIDDED_DATASETS
from dweather_client.aliases_and_units import snotel_to_ghcnd
import numpy as np
import pandas as pd
from io import StringIO
import datetime
from astropy import units as u
from astropy.units import imperial
import pytest


DAILY_DATASETS = [ds for ds in GRIDDED_DATASETS if "daily" in ds]
HOURLY_DATASETS = [ds for ds in GRIDDED_DATASETS if "hourly" in ds]
IPFS_TIMEOUT = 60

def test_get_gridcell_history_units():
    for s in DAILY_DATASETS + HOURLY_DATASETS:
        for use_imperial in [True, False]:
            res = get_gridcell_history(37, -83, s, use_imperial_units=use_imperial, ipfs_timeout=IPFS_TIMEOUT)
            for k in res:
                if res[k] is not None:
                    if "volumetric" in s:
                        assert res[k].unit == u.dimensionless_unscaled
                    elif use_imperial and ("precip" in s or "chirps" in s or "runoff" in s):
                        assert res[k].unit == imperial.inch
                    elif use_imperial and s == "rtma_pcp-hourly":
                        assert res[k].unit == imperial.pound / imperial.foot**2
                    elif use_imperial and "wind" in s:
                        assert res[k].unit == imperial.mile / u.hour
                    elif use_imperial:
                        assert res[k].unit == imperial.deg_F
                    elif s in {"era5_surface_runoff-hourly", "era5_land_precip-hourly"}:
                        assert res[k].unit == u.m
                    elif "precip" in s or "chirps" in s:
                        assert res[k].unit == u.mm
                    elif s == "rtma_pcp-hourly":
                        assert res[k].unit == u.kg / u.m**2
                    elif "wind" in s:
                        assert res[k].unit == u.m / u.s
                    else:
                        assert res[k].unit in (u.deg_C, u.K)

def test_get_gridcell_history_snap():
    lat_range = np.linspace(35, 40, 3)
    lon_range = np.linspace(-100, -80, 3)

    for s in DAILY_DATASETS:
        for lat in lat_range:
            for lon in lon_range:
                res = get_gridcell_history(lat, lon, s, also_return_snapped_coordinates=True, also_return_metadata=True, ipfs_timeout=IPFS_TIMEOUT)
                resolution, (snapped_lat, snapped_lon) = res[1]["metadata"]["resolution"], res[2]["snapped to"]
                assert abs(snapped_lat - lat) <= resolution
                assert abs(snapped_lon - lon) <= resolution

def test_get_gridcell_history_date_range():
    for s in DAILY_DATASETS:
        res = get_gridcell_history(37, -83, s, ipfs_timeout=IPFS_TIMEOUT)
        first_date, last_date = sorted(res)[0], sorted(res)[-1]
        diff = last_date - first_date
        assert diff.days == len(res) - 1
    for s in HOURLY_DATASETS:
        res = get_gridcell_history(37, -83, s, ipfs_timeout=IPFS_TIMEOUT)
        first_date, last_date = sorted(res)[0], sorted(res)[-1]
        time_diff = last_date - first_date
        time_diff_hours = time_diff.days * 24 + time_diff.seconds // 3600
        assert time_diff_hours + 1 == len(res)

def test_get_gridcell_nans():
    prism_r = get_gridcell_history(31.083, -120, "prismc-precip-daily", ipfs_timeout=IPFS_TIMEOUT)
    assert prism_r[datetime.date(1981, 8, 29)] is None

    rtma_r = get_gridcell_history(40.694754071664825, -73.93445989160746, "rtma_pcp-hourly", ipfs_timeout=IPFS_TIMEOUT)
    tz = next(iter(rtma_r)).tzinfo
    assert rtma_r[datetime.datetime(2011, 1, 29, 17, tzinfo=tz)] is None

def test_station():
    get_station_history('USW00014820', 'SNOW', ipfs_timeout=IPFS_TIMEOUT)
    get_station_history(snotel_to_ghcnd(838, 'CO'), 'snow water equivalent', ipfs_timeout=IPFS_TIMEOUT)
    get_station_history('USW00014820', 'TMAX', dataset='ghcnd-imputed-daily', ipfs_timeout=IPFS_TIMEOUT)
    get_station_history('USW00014820', 'TMIN', dataset='ghcnd-imputed-daily', ipfs_timeout=IPFS_TIMEOUT)
    get_station_history(snotel_to_ghcnd(602, 'CO'), 'WESD', ipfs_timeout=IPFS_TIMEOUT)

def test_storms_bad_args():
    with pytest.raises(ValueError):
        get_tropical_storms('simulated', 'NI', radius=100, ipfs_timeout=IPFS_TIMEOUT)
    with pytest.raises(ValueError):
        get_tropical_storms('simulated', 'NI', lat=100, ipfs_timeout=IPFS_TIMEOUT)
    with pytest.raises(ValueError):
        get_tropical_storms('simulated', 'NI', radius=500, lat=21, lon=65, min_lat=21, max_lat=22, min_lon=65, max_lon=66, ipfs_timeout=IPFS_TIMEOUT)

def test_simulated_storms():
    df_all_ni = get_tropical_storms('simulated', 'NI', ipfs_timeout=IPFS_TIMEOUT)
    df_subset_circle_ni = get_tropical_storms('simulated', 'NI', radius=500, lat=21, lon=65, ipfs_timeout=IPFS_TIMEOUT)
    df_subset_box_ni = get_tropical_storms('simulated', 'NI', min_lat=21, max_lat=22, min_lon=65, max_lon=66, ipfs_timeout=IPFS_TIMEOUT)

    assert len(df_all_ni.columns) == len(df_subset_circle_ni.columns) == len(df_subset_box_ni.columns) == 10
    assert len(df_subset_circle_ni) < len(df_all_ni)
    assert len(df_subset_box_ni) < len(df_all_ni)

def test_atcf_storms():
    df_all_al = get_tropical_storms('atcf', 'AL', ipfs_timeout=IPFS_TIMEOUT)
    df_subset_circle_al = get_tropical_storms('atcf', 'AL', radius=50, lat=26, lon=-90, ipfs_timeout=IPFS_TIMEOUT)
    df_subset_box_al = get_tropical_storms('atcf', 'AL', min_lat=26, max_lat=26.5, min_lon=-91, max_lon=-90.5, ipfs_timeout=IPFS_TIMEOUT)

    assert len(df_all_al.columns) == len(df_subset_circle_al.columns) == len(df_subset_box_al.columns) == 37
    assert len(df_subset_circle_al) < len(df_all_al)
    assert len(df_subset_box_al) < len(df_all_al)

def test_historical_storms():
    df_all_na = get_tropical_storms('historical', 'NA', ipfs_timeout=IPFS_TIMEOUT)
    df_subset_circle_na = get_tropical_storms('historical', 'NA', radius=50, lat=26, lon=-90, ipfs_timeout=IPFS_TIMEOUT)
    df_subset_box_na = get_tropical_storms('historical', 'NA', min_lat=26, max_lat=26.5, min_lon=-91, max_lon=-90.5, ipfs_timeout=IPFS_TIMEOUT)

    assert len(df_all_na.columns) == len(df_subset_circle_na.columns) == len(df_subset_box_na.columns) == 163
    assert len(df_subset_circle_na) < len(df_all_na)
    assert len(df_subset_box_na) < len(df_all_na)

def test_yields():
    df = pd.read_csv(StringIO(get_yield_history("0041", "12", "073", ipfs_timeout=IPFS_TIMEOUT)))
    assert len(df.columns) == 10
    assert len(df) >= 20

def test_power():
    power_dict = get_power_history(ipfs_timeout=IPFS_TIMEOUT)
    dict_length = len(power_dict) 
    assert dict_length >= 393885

    first_date, last_date = sorted(power_dict)[0], sorted(power_dict)[-1]
    time_diff = last_date - first_date
    time_diff_hours = time_diff.days * 48 + time_diff.seconds // 1800

    assert time_diff_hours + 1 == len(power_dict) 
    
def test_gas():
    power_dict = get_gas_history(ipfs_timeout=IPFS_TIMEOUT)
    dict_length = len(power_dict) 
    assert dict_length >= 6719

    first_date, last_date = sorted(power_dict)[0], sorted(power_dict)[-1]
    date_diff = last_date - first_date

    assert date_diff.days + 1 == len(power_dict) 

''' TODO some tests for RTMA behavior to be integrated into the new system
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
'''
