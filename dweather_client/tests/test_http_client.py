from dweather_client import http_client, utils
from datetime import date

def test_get_best_rtma_dict():
    client = http_client.RTMAClient()
    client.get_best_rtma_dict('49.01', '-125.01')

def test_http_client_rainfall():
    dataset = 'chirps_05'
    dataset_revision = 'chirps_05-daily'
    heads = http_client.get_heads()
    chirps_metadata = http_client.get_metadata(heads[dataset_revision])
    rainfall_dict = http_client.get_rainfall_dict(41.175, -75.125, dataset_revision)
    rainfall_rev_dict = http_client.get_rev_rainfall_dict(41.175, -75.125, dataset, date.today(), dataset_revision)

def test_http_client_temperature():
    dataset = "cpc_temp"
    dataset_revision = 'cpc_temp-daily'
    lat, lon = utils.conventional_lat_lon_to_cpc(41.25, -77.75)
    heads = http_client.get_heads()
    cpc_metadata = http_client.get_metadata(heads[dataset_revision])
    temperature_dict = http_client.get_temperature_dict(lat, lon, dataset_revision)
    temperature_rev_dict = http_client.get_rev_temperature_dict(lat, lon, dataset, date.today(), dataset_revision)
    tagged_temperature_rev_dict = http_client.get_rev_tagged_temperature_dict(lat, lon, dataset)

def test_get_station_csv():
    test_station = "ZI000067969"
    csv_str = http_client.get_station_csv(test_station)
    assert (len(csv_str.split()) > 100)

def test_parse_station_temps_as_dict():
    test_station = "ZI000067969"
    csv_str = http_client.get_station_csv(test_station)
    tmins, tmaxs = http_client.parse_station_temps_as_dict(csv_str)
    assert len(tmaxs) > 100
    assert len(tmins) > 100 