from dweather_client.client import get_station_history, get_gridcell_history
from dweather_client.aliases_and_units import snotel_to_ghcnd
import pandas as pd

def test_get_gridcell_history():
	#TODO test that units are correct, test that snapped coordinates and metadata are returned correctly
	# TODO test that dfs have correct column names and that columns are joined correctly
    get_gridcell_history(37, -83, "chirps_final_05-daily")
    get_gridcell_history(37, -83, "chirps_final_25-daily")
    get_gridcell_history(37, -83, "chirps_prelim_05-daily")
    get_gridcell_history(37, -83, "cpc_global-daily", also_return_metadata=True, also_return_snapped_coordinates=True)
    get_gridcell_history(37, -83, "cpc_temp-daily", also_return_metadata=True, also_return_snapped_coordinates=True)
    get_gridcell_history(37, -83, "cpc_us-daily", also_return_metadata=True, also_return_snapped_coordinates=True)


def test_station():
	# TODO test that units are correct and that colmns are joined correctly for DF
    get_station_history('USW00014820', 'SNOW')
    station_id = snotel_to_ghcnd(838, 'CO')
    get_station_history(station_id, 'snow water equivalent')
    temp_history = get_station_history('USW00014820', 'temperature')
    assert temp_history['TMIN'] != temp_history['TMAX']
    get_station_history('USW00014820', ('SNOW', 'SNWD', 'TMIN'), return_result_as_dataframe=True)