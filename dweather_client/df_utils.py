import numpy as np

def boxed_storms(df, min_lat, min_lon, max_lat, max_lon):
    return df[(df['lat'] >= min_lat) & (df['lon'] >= min_lon) & (df['lat'] <= max_lat) & (df['lon'] <= max_lon)]

def nearby_storms(df, c_lat, c_lon, radius): 
    """
    return:
        DataFrame with all time series points in `df` within `radius` in km of the point `(c_lat, c_lon)`
    args:
        c_lat (float): latitude coordinate of bounding circle
        c_lon (float): longitude coordinate of bounding circle
    """ 
    dist = haversine_vectorize(df['lon'], df['lat'], c_lon, c_lat)
    return df[dist < radius]

def haversine_vectorize(lon1, lat1, lon2, lat2):
    """
    Vectorized version of haversine great circle calculation. 
    return: 
        distance in km between `(lat1, lon1)` and `(lat2, lon2)`
    args:
        lon1 (float) first longitude coord
        lat1 (float) first latitude coord
        lon2 (float) second longitude coord
        lat2 (float) second latitude coord
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    newlon = lon2 - lon1
    newlat = lat2 - lat1
    haver_formula = np.sin(newlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(newlon/2.0)**2
    dist = 2 * np.arcsin(np.sqrt(haver_formula))
    km = 6367 * dist
    return km
