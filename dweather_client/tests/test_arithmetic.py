from dweather_client import arithmetic, df_arithmetic, df_loader, utils, df_utils
import datetime

TODAY = datetime.date.today()
ONE_MONTH = datetime.timedelta(days=30)


def test_build_historical_rainfall_dict():
    historical_raindict = arithmetic.build_historical_rainfall_dict(
        41.175, 
        -75.125, 
        'chirps_05-daily', 
        TODAY - (ONE_MONTH * 8),
        TODAY - (ONE_MONTH * 7),
    )


def test_sum_period_rainfall():
    a_sum = arithmetic.sum_period_rainfall(
         41.175, 
        -75.125, 
        'chirps_05-daily', 
        TODAY - (ONE_MONTH * 8),
        TODAY - (ONE_MONTH * 7),
    )


def test_sum_period_df():
    rainfall_df = df_loader.get_rainfall_df(41.125, -75.125, 'chirps_05-daily')
    sliced_period = df_utils.period_slice_df(
        rainfall_df, 
        TODAY - (ONE_MONTH * 8), 
        TODAY - (ONE_MONTH * 7), 
        30,
    )
    df_sum = df_arithmetic.sum_period_df(
        rainfall_df, 
        TODAY - (ONE_MONTH * 8), 
        TODAY - (ONE_MONTH * 7),
        30, 
        "PRCP")


def test_avg_period_df():
    rainfall_df = df_loader.get_rainfall_df(41.125, -75.125, 'chirps_05-daily')
    sliced_period = df_utils.period_slice_df(
        rainfall_df, 
        TODAY - (ONE_MONTH * 8), 
        TODAY - (ONE_MONTH * 7), 
        30,
    )
    df_avg = df_arithmetic.avg_period_df(
        rainfall_df, 
        TODAY - (ONE_MONTH * 8), 
        TODAY - (ONE_MONTH * 7),
        30, 
        "PRCP")
