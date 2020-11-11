from dweather_client import http_client, utils
from datetime import date

def test_station():
    station = http_client.get_station_csv("ACW00011604")
    station_content = '"ACW00011604","1949-01-12","17.11667","-61.78333","10.1","ST JOHNS COOLIDGE FIELD, AC","    0","T,,X","    0",",,X","    0",",,X","  278",",,X","  194",",,X",,,,,,,,,,,"    1",",,X"'
    assert(station_content in station)
