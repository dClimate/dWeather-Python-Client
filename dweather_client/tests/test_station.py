import http_client
import utils
from datetime import date

def test_station():
    station = http_client.get_station_csv("ACW00011604")