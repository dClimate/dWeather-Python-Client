from dweather_client.client import get_station_history
from dweather_client.aliases_and_units import snotel_to_ghcnd
import pandas as pd

def test_station():
    get_station_history('USW00014820', 'SNOW')
    station_id = snotel_to_ghcnd(838, 'CO')
    get_station_history(station_id, 'snow water equivalent')
    temp_history = get_station_history('USW00014820', 'temperature')
    assert temp_history['TMIN'] != temp_history['TMAX']
    get_station_history('USW00014820', ('SNOW', 'SNWD', 'TMIN'), return_result_as_dataframe=True)