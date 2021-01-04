from dweather_client.utils import build_rtma_reverse_lookup, build_rtma_lookup
from dweather_client import http_client
import requests, gzip, pickle, io, datetime

def add_to_bucket(buckets, bucket, lat, lon):
    try:
        buckets[bucket].append((lat, lon))
    except KeyError:
        buckets[bucket] = [(lat, lon)]

def add_to_buckets(buckets, bucket_size, lat, lon):
    add_to_bucket(buckets, (lat[:bucket_size], lon[:bucket_size]), lat, lon)
    if ((float(lat) - .15) < (int(lat[:bucket_size]) * (10**(2 - bucket_size)))): # if lat is close to a bucket border
        add_to_bucket(buckets, (str(int(lat[:bucket_size]) - 1), lon[:bucket_size]), lat, lon) # add to the neighboring bucket as well
    elif ((float(lat) + .15) > (int(lat[:bucket_size]) * (10**(2 - bucket_size)))):
        add_to_bucket(buckets, (str(int(lat[:bucket_size]) + 1), lon[:bucket_size]), lat, lon)
    if ((float(lon) - .15) < (int(lon[:bucket_size]) * (10**(2 - bucket_size)))):
        add_to_bucket(buckets, (lat[:bucket_size], str(int(lon[:bucket_size]) - 1)), lat, lon)
    elif ((float(lon) + .15) > (int(lon[:bucket_size]) * (10**(2 - bucket_size)))):
        add_to_bucket(buckets, (lat[:bucket_size], str(int(lon[:bucket_size]) + 1)), lat, lon)

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
    lat_lons = {}
    counter = 0
    try:
        for xy in xys:
            try:
                if (counter % 10000 == 0):
                    print('%s of %s' % (counter, len(xys)))
                lat = lookup['2016-01-06T14:00:00'][0][int(xy[1])][int(xy[0])]
                lon = lookup['2016-01-06T14:00:00'][1][int(xy[1])][int(xy[0])]
                add_to_buckets(lat_lons, 2, lat, lon)
            except IndexError:
                print("missing %s" % str(xy))
                continue
            counter = counter + 1
    finally:
        print("pickling")
        pickle.dump(lat_lons, open(str(datetime.datetime.now()) + "_rtma_lat_lons.p", "wb"))

if __name__ == "__main__":
    main()
