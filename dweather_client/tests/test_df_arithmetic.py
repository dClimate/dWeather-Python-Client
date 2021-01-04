from dweather_client.df_arithmetic import cat_icao_stations, cat_n_closest_station_dfs, get_polygon_df
from dweather_client.http_client import get_heads, get_metadata
from dweather_client.utils import snap_to_grid
from shapely.geometry import Point
import os

def test_get_polygon_df():
    brazil_states = \
        ['Acre', 'Alagoas', 'Amapá', 'Amazonas', 'Bahia', 'Ceará',
         'Distrito Federal', 'Espírito Santo', 'Goiás', 'Maranhão',
         'Mato Grosso', 'Mato Grosso do Sul', 'Minas Gerais', 'Paraná',
         'Paraíba', 'Pará', 'Pernambuco', 'Piauí', 'Rio Grande do Norte',
         'Rio Grande do Sul', 'Rio de Janeiro', 'Rondônia', 'Roraima',
         'Santa Catarina', 'Sergipe', 'São Paulo', 'Tocantins']
    dataset = 'chirps_prelim_05-daily'
    metadata = get_metadata(get_heads()[dataset])
    p1 = snap_to_grid(-9, -71.5, metadata)
    p2 = snap_to_grid(-8.75, -71.25, metadata)
    bounding_box = (Point(p1[1], p1[0]), Point(p2[1], p2[0]))

    get_polygon_df( \
        os.path.join(os.path.dirname(__file__), 'etc/gadm36_BRA_1.shp'),
        dataset,
        brazil_states,
        bounding_box,
        encoding='ISO-8859-1'
    )
        


#def test_cat_n_closest_station_dfs():
#    ghcndi_hash = get_heads()["ghcnd-imputed-daily"] 
#    stations = cat_n_closest_station_dfs( \
#        36, 
#        -94.5, 
#        10, 
#        pin=False, 
#        force_hash=ghcndi_hash
#    )
