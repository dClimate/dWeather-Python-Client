from dweather_client import ipfs_client, http_client, utils
from datetime import date


def test_ipfs_client_rainfall():
    dataset = 'chirps_05'
    dataset_revision = 'chirps_05-daily'
    heads = http_client.get_heads()
    chirps_metadata = ipfs_client.cat_metadata(heads[dataset_revision])
    ipfs_client.cat_rainfall_dict(41.175, -75.125, dataset_revision)
    rainfall_rev_dict = ipfs_client.cat_rev_rainfall_dict(41.175, -75.125, dataset, date.today(), dataset_revision)

def test_ipfs_client_temperature():
    dataset = "cpc_temp"
    dataset_revision = 'cpc_temp-daily'
    lat, lon = utils.conventional_lat_lon_to_cpc(41.25, -77.75)
    heads = http_client.get_heads()
    cpc_metadata = ipfs_client.cat_metadata(heads[dataset_revision])
    temperature_dict = ipfs_client.cat_temperature_dict(lat, lon, dataset_revision)
    temperature_rev_dict = ipfs_client.cat_rev_temperature_dict(lat, lon, dataset, date.today(), dataset_revision)
   
def test_cat_station_csv():
    test_station = "ZI000067969"
    csv_str = ipfs_client.cat_station_csv(test_station)
    assert(len(csv_str.split()) > 100)

def test_cat_station_df():
    test_station = "ZI000067969"
    csv_df = ipfs_client.cat_station_df(test_station)
    assert(csv_df.shape[0] > 100)


# TODO: test pin all stations?
# def test_pin_all_stations():
#    ipfs_client.pin_all_stations()

# This test takes a while.
'''def test_cat_icao_stations():
    station_dfs = ipfs_client.cat_icao_stations()
    assert len(station_dfs > 100)'''