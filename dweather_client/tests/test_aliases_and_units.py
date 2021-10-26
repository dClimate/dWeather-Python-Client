from dweather_client.aliases_and_units import snotel_to_ghcnd, rounding_formula

def test_snotel_to_ghcnd():
    assert snotel_to_ghcnd(602, 'CO') == 'USS0005K05S'
    assert snotel_to_ghcnd(838, 'CO') == 'USS0005J08S'


def test_rounding_formula():
    assert rounding_formula("10", 10, 25.5) == 26.0
    assert rounding_formula("34.60", 34.6, 88.23) == 88.23
