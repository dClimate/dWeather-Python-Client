import pandas as pd
from dweather_client.http_queries import get_simulated_hurricane_files, get_hurricane_dict, get_ibracs_hurricane_file
from dweather_client.df_utils import nearby_storms, boxed_storms


def get_simulated_hurricane_df(basin, **kwargs):
    """
    return:
        pd.DataFrame containing simulated hurricane data. If given kwargs radius, lat, lon,
        will subset df to only include points within radius in km of the point (lat, lon).
        Otherwise, if given kwargs 'min_lat', 'min_lon', 'max_lat', 'max_lon', selects points within
        a bounding box.
    args:
        basin (str), one of: EP, NA, NI, SI, SP or WP
    """
    files = get_simulated_hurricane_files(basin)
    dfs = [pd.read_csv(f, header=None)[range(10)] for f in files]
    df = pd.concat(dfs).reset_index(drop=True)
    columns = ['year', 'month', 'tc_num', 'time_step', 'basin', 'lat', 'lon', 'min_press', 'max_wind', 'rmw']
    df.columns = columns
    df.loc[df.lon > 180, 'lon'] = df.lon - 360
    if {'radius', 'lat', 'lon'}.issubset(kwargs.keys()):
        df = nearby_storms(df, kwargs['lat'], kwargs['lon'], kwargs['radius'])
    elif {'min_lat', 'min_lon', 'max_lat', 'max_lon'}.issubset(kwargs.keys()):
        df = boxed_storms(df, kwargs['min_lat'], kwargs['min_lon'], kwargs['max_lat'], kwargs["max_lon"])

    return df

def get_atcf_hurricane_df(basin, **kwargs):
    """
    return:
        pd.DataFrame containing ATCF historical hurricane data. If given kwargs radius, lat, lon,
        will subset df to only include points within radius in km of the point (lat, lon)
        Otherwise, if given kwargs 'min_lat', 'min_lon', 'max_lat', 'max_lon', selects points within
        a bounding box.
    args:
        basin (str), one of: AL, CP, EP, SL
    """
    if basin not in {'AL', 'CP', 'EP', 'SL'}:
        raise ValueError("Invalid basin ID")
    hist_dict = get_hurricane_dict()
    features = hist_dict['features']
    df_list = []
    for feature in features:
        hurr_dict = feature['properties']
        hurr_dict['lat'] = feature['geometry']['coordinates'][0]
        hurr_dict['lon'] = feature['geometry']['coordinates'][1]
        df_list.append(hurr_dict)
    df = pd.DataFrame(df_list)
    df = df[df["BASIN"] == basin]
    df['HOUR'] = pd.to_datetime(df["HOUR"])

    if {'radius', 'lat', 'lon'}.issubset(kwargs.keys()):
        df = nearby_storms(df, kwargs['lat'], kwargs['lon'], kwargs['radius'])
    elif {'min_lat', 'min_lon', 'max_lat', 'max_lon'}.issubset(kwargs.keys()):
        df = boxed_storms(df, kwargs['min_lat'], kwargs['min_lon'], kwargs['max_lat'], kwargs["max_lon"])

    for col in df:
        if col != "HOUR":
            df[col] = pd.to_numeric(df[col], errors='ignore')
    return df

def get_historical_hurricane_df(basin, **kwargs):
    """
    return:
        pd.DataFrame containing ibtracs historical hurricane data. More comprehensive than get_atcf_hurricane_df,
        but less frquently updated. If given kwargs radius, lat, lon,
        will subset df to only include points within radius in km of the point (lat, lon)
        Otherwise, if given kwargs 'min_lat', 'min_lon', 'max_lat', 'max_lon', selects points within
        a bounding box.
    args:
        basin (str), one of: 'NI', 'SI', 'NA', 'EP', 'WP', 'SP', 'SA'
    """
    df = pd.read_csv(get_ibracs_hurricane_file(), na_values=["", " "], keep_default_na=False, low_memory=False)
    df = df[1:]
    df = df[df['BASIN'] == basin]

    df["lat"] = df.LAT.astype(float)
    df["lon"] = df.LON.astype(float)

    del df["LAT"]
    del df["LON"]

    if {'radius', 'lat', 'lon'}.issubset(kwargs.keys()):
        df = nearby_storms(df, kwargs['lat'], kwargs['lon'], kwargs['radius'])
    elif {'min_lat', 'min_lon', 'max_lat', 'max_lon'}.issubset(kwargs.keys()):
        df = boxed_storms(df, kwargs['min_lat'], kwargs['min_lon'], kwargs['max_lat'], kwargs["max_lon"])

    df["HOUR"] = pd.to_datetime(df["ISO_TIME"])
    del df["ISO_TIME"]

    return df
