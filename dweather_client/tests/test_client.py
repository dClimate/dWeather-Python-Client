from dweather_client.client import get_station_dict
from dweather_client.aliases_and_units import snotel_to_ghcnd

def test_station():
    get_station_dict('USW00014820', 'SNOW')
    station_id = snotel_to_ghcnd(838, 'CO')
    get_station_dict(station_id, 'snow water equivalent')
    get_station_dict('USW00014820', 'TMAX')