from dweather_client.utils import build_rtma_reverse_lookup, build_rtma_lookup
from dweather_client import http_client
import requests, gzip, pickle, io

def main():
    heads = http_client.get_heads()
    rtma_hash = heads['rtma_pcp-hourly']
    print('getting grid_history')
    r = requests.get('https://gateway.arbolmarket.com/ipfs/%s/grid_history.txt.gz' % rtma_hash)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as grid_history_file:
        grid_history = grid_history_file.read().decode('utf-8')
    lookup = build_rtma_lookup(grid_history)
    r_loookup = build_rtma_reverse_lookup(grid_history)

    print('getting valid_coordinates')
    r = requests.get('https://gateway.arbolmarket.com/ipfs/%s/valid_coordinates.txt.gz' % rtma_hash)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as valid_coordinates_file:
        valid_coords = valid_coordinates_file.read().decode('utf-8')
    xys = {tuple(row.strip(')(').split(', ')) for row in valid_coords.split('\n')}
    lat_lons = []
    try:
        for xy in xys:
            try:
                if (len(lat_lons) % 10000 == 0):
                    print('%s of %s' % (len(lat_lons), len(xys)))
                lat = lookup['2016-01-06T14:00:00'][0][int(xy[1])][int(xy[0])]
                lon = lookup['2016-01-06T14:00:00'][1][int(xy[1])][int(xy[0])]
                lat_lons.append((lat, lon))
            except IndexError:
                print("missing %s" % str(xy))
                continue
    finally:
        print("pickling")
        pickle.dump(lat_lons, open("valid_lat_lons.p", "wb"))

if __name__ == "__main__":
    main()
