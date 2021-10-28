from dweather_client.ipfs_errors import *
from dweather_client.tests.mock_fixtures import get_patched_datasets
from dweather_client.client import get_station_history, get_gridcell_history, get_tropical_storms,\
    get_yield_history, get_irrigation_data, get_power_history, get_gas_history, get_alberta_power_history, GRIDDED_DATASETS, has_dataset_updated,\
    get_forecast_datasets, get_forecast, get_cme_station_history, get_european_station_history, get_drought_monitor_history
from dweather_client.aliases_and_units import snotel_to_ghcnd
import pandas as pd
from io import StringIO
import datetime
from astropy import units as u
from astropy.units import imperial
import pytest


DAILY_DATASETS = [ds for ds in GRIDDED_DATASETS if "daily" in ds]
HOURLY_DATASETS = [ds for ds in GRIDDED_DATASETS if "hourly" in ds]
IPFS_TIMEOUT = 60
ALTERNATE_METRIC_WIND_UNITS = "km / h"
ALTERNATE_IMPERIAL_WIND_UNITS = "mile / h"
PRECIP_UNITS = "ft"
BAD_UNIT = "basura"

def test_get_gridcell_history_units(mocker):
    mocker.patch("dweather_client.client.GRIDDED_DATASETS", get_patched_datasets())
    for s in DAILY_DATASETS + HOURLY_DATASETS:
        for use_imperial in [True, False]:
            res = get_gridcell_history(37, -83, s, use_imperial_units=use_imperial, ipfs_timeout=IPFS_TIMEOUT)
            for k in res:
                if res[k] is not None:
                    if "volumetric" in s:
                        assert res[k].unit == u.dimensionless_unscaled
                    elif use_imperial and ("precip" in s or "chirps" in s or "snowfall" in s or "runoff" in s):
                        assert res[k].unit == imperial.inch
                    elif use_imperial and s == "rtma_pcp-hourly":
                        assert res[k].unit == imperial.pound / imperial.foot**2
                    elif use_imperial and ("wind" in s or "gust" in s):
                        assert res[k].unit == imperial.mile / u.hour
                    elif use_imperial and "solar_radiation" in s:
                        assert res[k].unit == u.J / u.m ** 2
                    elif use_imperial:
                        assert res[k].unit == imperial.deg_F
                    elif s in {"era5_surface_runoff-hourly", "era5_land_precip-hourly", "era5_land_snowfall-hourly"}:
                        assert res[k].unit == u.m
                    elif "precip" in s or "chirps" in s:
                        assert res[k].unit == u.mm
                    elif s == "rtma_pcp-hourly":
                        assert res[k].unit == u.kg / u.m**2
                    elif "wind" in s or "gust" in s:
                        assert res[k].unit == u.m / u.s
                    elif "solar_radiation" in s:
                        assert res[k].unit == u.J / u.m ** 2
                    else:
                        assert res[k].unit in (u.deg_C, u.K)

def test_get_gridcell_history_desired_units(mocker):
    mocker.patch("dweather_client.client.GRIDDED_DATASETS", get_patched_datasets())
    res = get_gridcell_history(37, -83, "rtma_gust-hourly", desired_units=ALTERNATE_METRIC_WIND_UNITS)
    for k in res:
        if res[k] is not None:
            assert res[k].unit == u.km / u.h

def test_get_gridcell_history_desired_units_imperial(mocker):
    mocker.patch("dweather_client.client.GRIDDED_DATASETS", get_patched_datasets())
    res = get_gridcell_history(37, -83, "rtma_gust-hourly", desired_units=ALTERNATE_IMPERIAL_WIND_UNITS)
    for k in res:
        if res[k] is not None:
            assert res[k].unit == imperial.mile / u.h

def test_get_gridcell_history_desired_temperature(mocker):
    mocker.patch("dweather_client.client.GRIDDED_DATASETS", get_patched_datasets())
    res = get_gridcell_history(37, -83, "rtma_temp-hourly", desired_units="deg_F")
    for k in res:
        if res[k] is not None:
            assert res[k].unit == imperial.deg_F

def test_get_gridcell_history_desired_units_incompatible(mocker):
    mocker.patch("dweather_client.client.GRIDDED_DATASETS", get_patched_datasets())
    with pytest.raises(UnitError):
        get_gridcell_history(37, -83, "rtma_gust-hourly", desired_units=PRECIP_UNITS)

def test_get_gridcell_history_desired_unit_not_found(mocker):
    mocker.patch("dweather_client.client.GRIDDED_DATASETS", get_patched_datasets())
    with pytest.raises(UnitError):
        get_gridcell_history(37, -83, "rtma_gust-hourly", desired_units=BAD_UNIT)

def test_get_forecast_units():
    for s in get_forecast_datasets():
        for use_imperial in [True, False]:
            res = get_forecast(37, -83, datetime.date(2021, 8, 20), s, use_imperial_units=use_imperial, ipfs_timeout=IPFS_TIMEOUT)["data"]
            for k in res:
                if res[k] is not None:
                    if "volumetric" in s:
                        assert res[k].unit == u.dimensionless_unscaled
                    elif "humidity" in s:
                        assert res[k].unit == u.pct
                    elif "pcp_rate" in s:
                        assert res[k].unit == u.kg / (u.m **2 ) / u.s
                    elif use_imperial and "wind" in s:
                        assert res[k].unit == imperial.mile / u.hour
                    elif use_imperial:
                        assert res[k].unit == imperial.deg_F
                    elif "wind" in s:
                        assert res[k].unit == u.m / u.s
                    else:
                        assert res[k].unit == u.K

def test_get_forecast_desired_units():
    res = get_forecast(37, -83, datetime.date(2021, 8, 20), "gfs_10m_wind_u-hourly", desired_units=ALTERNATE_METRIC_WIND_UNITS, ipfs_timeout=IPFS_TIMEOUT)["data"]
    for k in res:
        if res[k] is not None:
            assert res[k].unit == u.km / u.h

def test_get_forecast_desired_units_imperial():
    res = get_forecast(37, -83, datetime.date(2021, 8, 20), "gfs_10m_wind_u-hourly", desired_units=ALTERNATE_IMPERIAL_WIND_UNITS, ipfs_timeout=IPFS_TIMEOUT)["data"]
    for k in res:
        if res[k] is not None:
            assert res[k].unit == imperial.mile / u.h

def test_get_forecast_desired_temperature():
    res = get_forecast(37, -83, datetime.date(2021, 8, 20), "gfs_tmax-hourly", desired_units="deg_F", ipfs_timeout=IPFS_TIMEOUT)["data"]
    for k in res:
        if res[k] is not None:
            assert res[k].unit == imperial.deg_F

def test_get_forecast_desired_units_incompatible():
    with pytest.raises(UnitError):
        get_forecast(37, -83, datetime.date(2021, 8, 20), "gfs_10m_wind_u-hourly", desired_units=PRECIP_UNITS)

def test_get_forecast_desired_unit_not_found():
    with pytest.raises(UnitError):
        get_forecast(37, -83, datetime.date(2021, 8, 20), "gfs_10m_wind_u-hourly", desired_units=BAD_UNIT)

def test_get_gridcell_history_date_range(mocker):
    mocker.patch("dweather_client.client.GRIDDED_DATASETS", get_patched_datasets())
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

def test_get_forecast_date_range():
    for s in get_forecast_datasets():
        res = get_forecast(37, -83, datetime.date(2021, 8, 20), s, ipfs_timeout=IPFS_TIMEOUT)["data"]
        first_date, last_date = sorted(res)[0], sorted(res)[-1]
        time_diff = last_date - first_date
        time_diff_hours = time_diff.days * 24 + time_diff.seconds // 3600
        assert time_diff_hours + 1 == len(res) == 16 * 24

def test_get_gridcell_nans(mocker):
    mocker.patch("dweather_client.client.GRIDDED_DATASETS", get_patched_datasets())
    prism_r = get_gridcell_history(31.083, -120, "prismc-precip-daily", ipfs_timeout=IPFS_TIMEOUT)
    assert prism_r[datetime.date(1981, 8, 29)] is None

    rtma_r = get_gridcell_history(40.694754071664825, -73.93445989160746, "rtma_pcp-hourly", ipfs_timeout=IPFS_TIMEOUT)
    tz = next(iter(rtma_r)).tzinfo
    assert rtma_r[datetime.datetime(2011, 1, 29, 17, tzinfo=tz)] is None

def test_gridcell_as_of():
    prism_small = get_gridcell_history(31.083, -120, "prismc-precip-daily", as_of=datetime.datetime(2021, 4, 30), ipfs_timeout=IPFS_TIMEOUT)
    first_date, last_date = sorted(prism_small)[0], sorted(prism_small)[-1]
    diff = last_date - first_date
    assert diff.days == len(prism_small) - 1
    prism_full = get_gridcell_history(31.083, -120, "prismc-precip-daily", ipfs_timeout=IPFS_TIMEOUT)
    assert len(prism_small) < len(prism_full)

def test_station():
    get_station_history('USW00014820', 'SNOW', ipfs_timeout=IPFS_TIMEOUT)
    get_station_history(snotel_to_ghcnd(838, 'CO'), 'snow water equivalent', ipfs_timeout=IPFS_TIMEOUT)
    get_station_history('USW00014820', 'TMAX', dataset='ghcnd-imputed-daily', ipfs_timeout=IPFS_TIMEOUT)
    get_station_history('USW00014820', 'TMIN', dataset='ghcnd-imputed-daily', ipfs_timeout=IPFS_TIMEOUT)
    get_station_history(snotel_to_ghcnd(602, 'CO'), 'WESD', ipfs_timeout=IPFS_TIMEOUT)

def test_station_desired_units():
    res = get_station_history('USW00014820', 'TMAX', desired_units="K", ipfs_timeout=IPFS_TIMEOUT)
    for k in res:
        assert res[k].unit == u.K

def test_station_bad_units():
    with pytest.raises(UnitError):
       get_station_history('USW00014820', 'TMAX', desired_units="blah", ipfs_timeout=IPFS_TIMEOUT)

def test_station_incompatible_units():
    with pytest.raises(UnitError):
       get_station_history('USW00014820', 'TMAX', desired_units="m", ipfs_timeout=IPFS_TIMEOUT)
    
def test_cme_station():
    cme = get_cme_station_history('47662', 'TMAX', use_imperial_units=True, ipfs_timeout=IPFS_TIMEOUT)
    assert len(cme) >= 22171
    assert cme[datetime.date(1962, 8, 2)].unit == imperial.deg_F

def test_cme_station_desired_units():
    cme = get_cme_station_history('47662', 'TMAX', desired_units="K", ipfs_timeout=IPFS_TIMEOUT)
    assert cme[datetime.date(1962, 8, 2)].unit == u.K

def test_dutch_station():
    dutch = get_european_station_history('dutch_stations-daily', '215', 'TMIN', use_imperial_units=True, ipfs_timeout=IPFS_TIMEOUT)
    assert len(dutch) >= 2626
    assert dutch[datetime.date(2017, 3, 29)].unit == imperial.deg_F

def test_german_station():
    german = get_european_station_history('dwd_stations-daily', '13670', 'TMIN', use_imperial_units=True, ipfs_timeout=IPFS_TIMEOUT)
    assert len(german) >= 5234
    assert german[datetime.date(2017, 3, 29)].unit == imperial.deg_F

def test_european_station_desired_units():
    german = get_european_station_history('dwd_stations-daily', '13670', 'TMIN', desired_units="K", ipfs_timeout=IPFS_TIMEOUT)
    assert german[datetime.date(2017, 3, 29)].unit == u.K

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

    assert len(df_all_ni.columns) == len(df_subset_circle_ni.columns) == len(df_subset_box_ni.columns) == 11
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

def test_irrigation():
    df = pd.read_csv(StringIO(get_irrigation_data("0041", ipfs_timeout=IPFS_TIMEOUT)))
    assert len(df.columns) == 4
    assert len(df) > 0

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

def test_aeso_power():
    power_dict = get_alberta_power_history(ipfs_timeout=IPFS_TIMEOUT)
    dict_length = len(power_dict) 
    assert dict_length >= 188098

    first_date, last_date = sorted(power_dict)[0], sorted(power_dict)[-1]
    time_diff = last_date - first_date
    time_diff_hours = time_diff.days * 24 + time_diff.seconds // 3600

    assert time_diff_hours + 1 == len(power_dict) 

def test_drought_monitor():
    drought_dict = get_drought_monitor_history("48", "071", ipfs_timeout=IPFS_TIMEOUT)
    dict_length = len(drought_dict) 
    assert dict_length >= 42

    first_date, last_date = sorted(drought_dict)[0], sorted(drought_dict)[-1]
    time_diff = last_date - first_date
    time_diff_weeks = time_diff.days / 7

    assert time_diff_weeks + 1 == len(drought_dict) 

def test_has_dataset_updated_true():
    assert has_dataset_updated(
        "era5_wind_100m_u-hourly", 
        [[datetime.datetime(2021, 4, 3), datetime.datetime(2021, 5, 3)]],
        datetime.datetime(2021, 7, 25), 
        ipfs_timeout=10
    )

def test_has_dataset_updated_false():
    assert not has_dataset_updated(
        "era5_wind_100m_u-hourly", 
        [[datetime.datetime(1990, 4, 3), datetime.datetime(2000, 5, 3)]],
        datetime.datetime(2021, 7, 25), 
        ipfs_timeout=10
    )