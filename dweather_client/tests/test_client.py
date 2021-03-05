from dweather_client.client import get_station_history, get_gridcell_history
from dweather_client.aliases_and_units import snotel_to_ghcnd
import pandas as pd
import numpy as np
from astropy import units as u
from astropy.units import imperial

DAILY_DATASETS = [
    "chirpsc_final_05-daily",
    "chirpsc_final_25-daily",
    "chirpsc_prelim_05-daily",
    "cpcc_precip_global-daily",
    "cpcc_precip_us-daily",
    "cpcc_temp_max-daily",
    "cpcc_temp_min-daily",
    "prismc-tmax-daily",
    "prismc-tmin-daily",
    "prismc-precip-daily"
]

HOURLY_DATASETS = [
    "rtma_pcp-hourly"
]

def test_get_gridcell_history_units():
    for s in DAILY_DATASETS + HOURLY_DATASETS:
        for use_imperial in [True, False]:
            res = get_gridcell_history(37, -83, s, use_imperial_units=use_imperial)
            for k in res:
                if use_imperial and ("precip" in s or "chirps" in s):
                    assert res[k].unit == imperial.inch
                elif use_imperial and s == "rtma_pcp-hourly":
                    assert res[k].unit == imperial.pound / imperial.foot**2
                elif use_imperial:
                    assert res[k].unit == imperial.deg_F
                elif "precip" in s or "chirps" in s:
                    assert res[k].unit == u.mm
                elif s == "rtma_pcp-hourly":
                    assert res[k].unit == u.kg / u.m**2
                else:
                    assert res[k].unit == u.deg_C

def test_get_gridcell_history_snap():
    lat_range = np.linspace(35, 40, 3)
    lon_range = np.linspace(-100, -80, 3)

    for s in DAILY_DATASETS:
        for lat in lat_range:
            for lon in lon_range:
                res = get_gridcell_history(lat, lon, s, also_return_snapped_coordinates=True, also_return_metadata=True)
                resolution, (snapped_lat, snapped_lon) = res[1]["metadata"]["resolution"], res[2]["snapped to"]
                assert abs(snapped_lat - lat) <= resolution
                assert abs(snapped_lon - lon) <= resolution

def test_get_gridcell_history_date_range():
    for s in DAILY_DATASETS:
        res = get_gridcell_history(37, -83, s)
        first_date, last_date = sorted(res)[0], sorted(res)[-1]
        diff = last_date - first_date
        assert diff.days == len(res) - 1
    for s in HOURLY_DATASETS:
        res = get_gridcell_history(37, -83, s)
        first_date, last_date = sorted(res)[0], sorted(res)[-1]
        time_diff = last_date - first_date
        time_diff_hours = time_diff.days * 24 + time_diff.seconds // 3600
        assert time_diff_hours + 1 == len(res)

    # era5_land_wind_u-hourly TODO
    # era5_land_wind_v-hourly TODO

def test_station():
    # TODO test that units are correct and that columns are joined correctly for DF
    get_station_history('USW00014820', 'SNOW')
    get_station_history(snotel_to_ghcnd(838, 'CO'), 'snow water equivalent')
    get_station_history('USW00014820', 'TMAX', dataset='ghcnd-imputed-daily')
    get_station_history('USW00014820', 'TMIN', dataset='ghcnd-imputed-daily')
    get_station_history('USW00014820', 'WESD', return_result_as_dataframe=True)


'''
def test_storm(): TODO
   atcf_btk-seasonal
   ibtracs-tropical-storm
   storm-simulated-hurricane
   teleconnections-el-nino-monthly

def test_yield(): TODO
    nass_corn-yearly
    nass_soybeans-yearly
    sco-yearly
'''
