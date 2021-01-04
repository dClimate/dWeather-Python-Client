from dweather_client import http_client, utils
from datetime import date

def test_station():
    station = http_client.get_station_csv("ASN00010073")
    assert(len(station.split()) > 365)
