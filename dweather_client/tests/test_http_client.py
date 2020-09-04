import http_client
import utils

def test_http_client_rainfall():
    dataset = 'chirps_05-daily'
    heads = http_client.get_heads()
    chirps_metadata = http_client.get_metadata(heads[dataset])
    rainfall_dict = http_client.get_rainfall_dict(41.175, -75.125, dataset)


def test_http_client_temperature():
    dataset = 'cpc_temp-daily'
    lat, lon = utils.conventional_lat_lon_to_cpc(41.25, -77.75)
    heads = http_client.get_heads()
    cpc_metadata = http_client.get_metadata(heads[dataset])
    temperature_dict = http_client.get_temperature_dict(lat, lon, dataset)


