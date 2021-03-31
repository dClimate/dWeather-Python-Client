from dweather_client import df_loader

def test_get_simulated_hurricane_df():
    df_all_ni = df_loader.get_simulated_hurricane_df('NI')
    df_subset_circle_ni = df_loader.get_simulated_hurricane_df('NI', radius=500, lat=21, lon=65)
    df_subset_box_ni = df_loader.get_simulated_hurricane_df('NI', min_lat=21, max_lat=22, min_lon=65, max_lon=66)

    assert len(df_all_ni.columns) == len(df_subset_circle_ni.columns) == len(df_subset_box_ni.columns) == 10
    assert len(df_subset_circle_ni) < len(df_all_ni)
    assert len(df_subset_box_ni) < len(df_all_ni)

def test_get_historical_hurricane_df():
    df_all_na = df_loader.get_historical_hurricane_df('NA')
    df_subset_circle_na = df_loader.get_historical_hurricane_df('NA', radius=50, lat=26, lon=-90)
    df_subset_box_na = df_loader.get_historical_hurricane_df('NA', min_lat=26, max_lat=26.5, min_lon=-91, max_lon=-90.5)

    assert len(df_all_na.columns) == len(df_subset_circle_na.columns) == len(df_subset_box_na.columns) == 163
    assert len(df_subset_circle_na) < len(df_all_na)
    assert len(df_subset_box_na) < len(df_all_na)

def test_get_atcf_hurricane_df():
    df_all_al = df_loader.get_atcf_hurricane_df('AL')
    df_subset_circle_al = df_loader.get_atcf_hurricane_df('AL', radius=50, lat=26, lon=-90)
    df_subset_box_al = df_loader.get_atcf_hurricane_df('AL', min_lat=26, max_lat=26.5, min_lon=-91, max_lon=-90.5)

    assert len(df_all_al.columns) == len(df_subset_circle_al.columns) == len(df_subset_box_al.columns) == 37
    assert len(df_subset_circle_al) < len(df_all_al)
    assert len(df_subset_box_al) < len(df_all_al)
