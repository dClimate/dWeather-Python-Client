from dweather_client.utils import build_rtma_reverse_lookup, build_rtma_lookup
from dweather_client import http_client
import requests, gzip, pickle, io, datetime

def add_to_bucket(buckets, bucket, lat, lon):
    try:
        buckets[bucket].append((lat, lon))
    except KeyError:
        buckets[bucket] = [(lat, lon)]

def add_to_buckets(buckets, offset, lat, lon):
    """
    We are taking an argument "buckets" and adding a new element to it.

    The element is (lat, lon).
    
    The point is to take a giant dict and break it down into sub-dicts.
    The key to each sub-dict is the integer components of the lat/lon coordinates
    Points that are in nearby grid squares are duplicated in other dicts
    """
    float_lat = float(lat)
    float_lon = float(lon)
    int_part_of_lat = int(float_lat)
    int_part_of_lon = int(float_lon)
    
    # Always add to main bucket
    add_to_bucket(buckets, (str(int_part_of_lat), str(int_part_of_lon)), lat, lon)

    # Determine whether the coord is close enough to the boundary to add to other buckets
    close_to_higher_lat = float_lat > 1 + int_part_of_lat - offset
    close_to_lower_lat = float_lat < int_part_of_lat + offset
    close_to_higher_lon = float_lon > 1 + int_part_of_lon - offset
    close_to_lower_lon = float_lon < int_part_of_lon + offset

    if close_to_higher_lat:
        add_to_bucket(buckets, (str(int_part_of_lat + 1), str(int_part_of_lon)), lat, lon)
    if close_to_higher_lon:
        add_to_bucket(buckets, (str(int_part_of_lat), str(int_part_of_lon + 1)), lat, lon)
    if close_to_lower_lat:
        add_to_bucket(buckets, (str(int_part_of_lat - 1), str(int_part_of_lon)), lat, lon)
    if close_to_lower_lon:
        add_to_bucket(buckets, (str(int_part_of_lat), str(int_part_of_lon - 1)), lat, lon)
    if close_to_higher_lat and close_to_higher_lon:
        add_to_bucket(buckets, (str(int_part_of_lat + 1), str(int_part_of_lon + 1)), lat, lon)
    if close_to_higher_lon and close_to_lower_lat:
        add_to_bucket(buckets, (str(int_part_of_lat - 1), str(int_part_of_lon + 1)), lat, lon)
    if close_to_lower_lat and close_to_lower_lon:
        add_to_bucket(buckets, (str(int_part_of_lat - 1), str(int_part_of_lon - 1)), lat, lon)
    if close_to_lower_lat and close_to_higher_lon:
        add_to_bucket(buckets, (str(int_part_of_lat - 1), str(int_part_of_lon + 1)), lat, lon)

def main():
    heads = http_client.get_heads()
    rtma_hash = heads['rtma_pcp-hourly']
    print('getting grid_history')
    
    # Although the RTMA grid did change once in the past, we assume it will never change again.
    # fingers crossed emoji.
    r = requests.get('https://gateway.arbolmarket.com/ipfs/%s/grid_history.txt.gz' % rtma_hash)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as grid_history_file:
        grid_history = grid_history_file.read().decode('utf-8')

    # lookup is converting grid_history into a dict {(x, y): (lat, lon)}
    lookup = build_rtma_lookup(grid_history)

    print('getting valid_coordinates')
    r = requests.get('https://gateway.arbolmarket.com/ipfs/%s/valid_coordinates.txt.gz' % rtma_hash)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as valid_coordinates_file:
        valid_coords = valid_coordinates_file.read().decode('utf-8')
        
    # this is a SET!! of every valid (x, y)
    xys = {tuple(row.strip(')(').split(', ')) for row in valid_coords.split('\n')}
    lat_lons = {}
    counter = 0
    try:
        for xy in xys:
            try:
                if (counter % 10000 == 0):
                    print('%s of %s' % (counter, len(xys)))
                    
                # Get a (lat, lon) for a given (x, y)
                lat = lookup['2016-01-06T14:00:00'][0][int(xy[1])][int(xy[0])]
                lon = lookup['2016-01-06T14:00:00'][1][int(xy[1])][int(xy[0])]
                
                # Once we have a valid (lat, lon) we want to save it.
                add_to_buckets(lat_lons, 0.15, lat, lon)
            except IndexError:
                print("missing %s" % str(xy))
                continue
            counter = counter + 1
    finally:
        print("pickling")
        pickle.dump(lat_lons, open(str(datetime.datetime.now()) + "_rtma_lat_lons.p", "wb"))

if __name__ == "__main__":
    main()
