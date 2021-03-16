import pickle, gzip, os, math
from dweather_client.struct_utils import find_closest_lat_lon

def test_generate_rtma_lat_lons():
    with gzip.open(os.path.join(os.path.dirname(__file__), "../etc/rtma_lat_lons.p.gz"), "rb") as gz:
        valid_lat_lons = pickle.load(gz)
    lat, lon = "49.32", "239.79"
    closest = find_closest_lat_lon(valid_lat_lons[(lat[:2], lon[:3])], (lat, lon))
    # Correct values are pulled straight from the RTMA GRIB2 files.
    assert math.isclose(float(closest[0]), 49.320071007418235)
    assert math.isclose(float(closest[1]), 239.7957335374897)