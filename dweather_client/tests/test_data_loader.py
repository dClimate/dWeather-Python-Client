from dweather_client import http_client, utils
from dweather_client.data_loader import GridCellDataLoader
import datetime

def test_get_revision_rainfall():
    dataset = "chirps_05"
    dataset_revision = "chirps_prelim_05-daily"
    coords = 41.175, -75.125
    loader = GridCellDataLoader(dataset, *coords, "rainfall")
    http_client_meta, http_client_dict = http_client.get_rainfall_dict(*coords, dataset_revision, return_metadata=True)
    loader_data = loader.get_revision(dataset_revision)
    loader_dict = loader_data["data"]
    loader_meta = loader_data["metadata"]
    assert http_client_dict == loader_dict
    assert http_client_meta == loader_meta


def test_get_revision_temperature():
    dataset = "prism_temp"
    dataset_revision = 'prism_temp-daily'
    coords=41.25, -77.75
    loader = GridCellDataLoader(dataset, *coords, "temperature")
    http_client_meta, http_client_highs, http_client_lows = http_client.get_temperature_dict(*coords, dataset_revision, return_metadata=True)
    loader_data = loader.get_revision(dataset_revision)
    loader_dict = loader_data["data"]
    loader_meta = loader_data["metadata"]
    assert http_client_highs == loader_dict["highs"]
    assert http_client_lows == loader_dict["lows"]
    assert http_client_meta == loader_meta

def test_multi_revision_dict_rainfall():
    dataset = "prism_precip"
    coords = 41.25, -77.75
    loader = GridCellDataLoader(dataset, *coords, "rainfall")
    loader_rain = loader.build_multi_revision_dict()
    http_rain, _ = http_client.get_rev_rainfall_dict(*coords, dataset, datetime.date(2020, 1, 1), "prism_rev_1_precip-daily")

    for date in http_rain:
        assert loader_rain[date][0] == http_rain[date]


def test_multi_revision_dict_temperature():
    dataset = "prism_temp"
    coords = 41.25, -77.75
    loader = GridCellDataLoader(dataset, *coords, "temperature")
    loader_temps = loader.build_multi_revision_dict()
    http_highs, http_lows = http_client.get_rev_tagged_temperature_dict(*coords, dataset, datetime.date(2020, 1, 1))

    for date in http_highs:
        assert loader_temps["highs"][date][0] == http_highs[date][0]
        assert loader_temps["lows"][date][0] == http_lows[date][0]

def test_single_instance_enforcement():
    chirps_args = "chirps_05", 41.625, -93.125, "rainfall"
    loader_a = GridCellDataLoader(*chirps_args)
    loader_b = GridCellDataLoader(*chirps_args)
    assert loader_a is loader_b
    prism_args = "prism_precip", 41.25, -77.75, "rainfall"
    loader_c = GridCellDataLoader(*prism_args)
    assert loader_a is not loader_c

