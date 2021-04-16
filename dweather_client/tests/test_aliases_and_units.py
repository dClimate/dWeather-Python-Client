from dweather_client.aliases_and_units import *

def test_snotel_to_ghcnd():
    assert snotel_to_ghcnd(602, 'CO') == 'USS0005K05S'
    assert snotel_to_ghcnd(838, 'CO') == 'USS0005J08S'

