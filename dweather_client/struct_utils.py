import math

def tupleify(args):
    if isinstance(args, tuple):
        return args
    return (args,)

def find_closest_lat_lon(lst, K):
    """
    Find the closest (lat, lon) tuple in a list to a given 
    (lat, lon) tuple K. Use euclidian distance for performance reasons.
    """
    return lst[min(range(len(lst)), key = lambda i: math.sqrt((float(lst[i][0]) - float(K[0]))**2 + (float(lst[i][1]) - float(K[1]))**2 ))] 
