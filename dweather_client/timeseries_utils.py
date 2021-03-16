from datetime import datetime
import pandas as pd


def listify_period(start_date, end_date):
    """
    Make a list of all dates from start_date to end_date, inclusive.
    Args:
        start_date (datetime.date): first date to include
        end_date (datetime.date): last date to include
    Returns:
        list of datetime.date objects
    """
    days_in_range = (end_date - start_date).days
    return [start_date + datetime.timedelta(n) for n in range(days_in_range + 1)]

def period_slice_df(df, ps, pe, yrs):
    """ 
    Slice a dataframe by period where n is based on years lookback 
    
    return:
        pd.DataFrame()
    
    args:
        df = any pd.DataFrame() with a datetime.date() index
        ps = datetime.date object (contract period start)
        ps = datetime.date object (contract period end)
        yrs = 30 (years lookback)
        
    limitations:
        max period is 364 days
        
    """

    mydf = pd.DataFrame()
    
    leftDay = ~((df.index.day < ps.day) & (df.index.month == ps.month))
    rightDay = ~((df.index.day > pe.day) & (df.index.month == pe.month))
    #first year just get end
    if ps.year < pe.year:
        firstYear = ps.year - yrs
        yr = df.index.year == firstYear
        mon = df.index.month >= ps.month
        #a boolean mask to cut the left and right half month days if need be
        mydf = mydf.append(df[(yr)&(mon)&(leftDay)])
        #everything in betweeng
        for yrss in range(ps.year - (yrs - 1), pe.year - 1):
            ##boolean mask for now, better method needed##
            yearChunk = (df.index.year == yrss)&((df.index.month<=pe.month)|(df.index.month>=ps.month))
            yearChunk = (yearChunk)&leftDay&rightDay
            mydf = mydf.append(df[yearChunk])
        #build last year
        mydf = mydf.append(df[rightDay&(df.index.month<=pe.month)&(df.index.year==pe.year-1)])
    else:
        mydf = df[leftDay&rightDay&(df.index.month>=ps.month)&(df.index.month<=pe.month)&(df.index.year>=ps.year-yrs)]
            
    return mydf

