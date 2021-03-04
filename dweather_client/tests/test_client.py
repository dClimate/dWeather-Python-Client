from dweather_client.client import get_station_history, get_gridcell_history
from dweather_client.aliases_and_units import snotel_to_ghcnd
import pandas as pd

def test_get_gridcell_history():
	#TODO test that units are correct, test that snapped coordinates and metadata are returned correctly
	# TODO test that dfs have correct column names and that columns are joined correctly

    get_gridcell_history(37, -83, "chirpsc_final_05-daily")
    get_gridcell_history(37, -83, "chirpsc_final_25-daily")
    get_gridcell_history(37, -83, "chirpsc_prelim_05-daily")
#    get_gridcell_history(37, -83, "cpcc_temp_max-daily")
 #   get_gridcell_history(37, -83, "cpcc_temp_min-daily")
  #  get_gridcell_history(37, -83, "cpcc_precip_global-daily")
   # get_gridcell_history(37, -83, "cpcc_precip_us-daily")
    get_gridcell_history(37, -83, "prismc-precip-daily")
    get_gridcell_history(37, -83, "prismc-tmax-daily")
    get_gridcell_history(37, -83, "prismc-tmin-daily")
    get_gridcell_history(37, -83, "rtma_pcp-hourly")
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