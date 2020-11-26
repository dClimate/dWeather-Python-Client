from dweather_client import http_client

def test_hurricane():
    default_dict = http_client.get_hurricane_dict()
    assert(len(default_dict['features']) >= 93058)