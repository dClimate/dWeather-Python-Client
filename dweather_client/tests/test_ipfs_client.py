from dweather_client import ipfs_client, http_client, utils
from datetime import date

def test_ipfs_client_rainfall():
    dataset = 'chirps_05'
    dataset_revision = 'chirps_05-daily'
    heads = http_client.get_heads()
    chirps_metadata = ipfs_client.cat_metadata(heads[dataset_revision])
    ipfs_client.cat_rainfall_dict(41.175, -75.125, dataset_revision)
    rainfall_rev_dict = ipfs_client.cat_rev_rainfall_dict(41.175, -75.125, dataset, date.today(), dataset_revision)

def test_http_client_temperature():
    dataset = "cpc_temp"
    dataset_revision = 'cpc_temp-daily'
    lat, lon = utils.conventional_lat_lon_to_cpc(41.25, -77.75)
    heads = http_client.get_heads()
    cpc_metadata = ipfs_client.cat_metadata(heads[dataset_revision])
    temperature_dict = ipfs_client.cat_temperature_dict(lat, lon, dataset_revision)
    temperature_rev_dict = ipfs_client.cat_rev_temperature_dict(lat, lon, dataset, date.today(), dataset_revision)
   
