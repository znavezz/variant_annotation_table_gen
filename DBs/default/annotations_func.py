import pandas as pd

def isAdARFixable(df: pd.DataFrame) -> pd.Series:
    """
    Function to determine if a variant is ADAR fixable.
    """
    df["isAdARFixable"] = (df["ref"].str.upper() == "G") & (df["alt"].str.upper() == "A")
    return df["isAdARFixable"]

def isApoBecFixable(df: pd.DataFrame) -> pd.Series:
    """
    Function to determine if a variant is ApoBec fixable.
    """
    df["isApoBecFixable"] = df["ref"].str.upper() == "T" and df["alt"].str.upper() == "C"
    return df["isApoBecFixable"]