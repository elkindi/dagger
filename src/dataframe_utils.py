import numpy as np
import pandas as pd
from datetime import time, date, datetime
"""
Dictionary with mapping from python,
pandas and numpy types to postgres types

Types not listed in here are all mapped to 'text'
"""
conversion_table = {
    int: 'integer',
    float: 'double precision',
    str: 'text',
    bool: 'bool',
    date: 'date',
    time: 'time',
    datetime: 'timestamp without time zone',
    np.object: 'text',
    np.int8: 'smallint',
    np.int16: 'smallint',
    np.int32: 'integer',
    np.int64: 'bigint',
    np.uint8: 'smallint',
    np.uint16: 'integer',
    np.uint32: 'bigint',
    np.float32: 'real',
    np.float64: 'double precision',
    np.datetime64: 'timestamp without time zone',
    np.timedelta64: 'interval',
    pd.Categorical: 'text'
}


def get_df_cols_rows(df: pd.DataFrame, type_map=True):
    """
    Returns the list of columns (including the index)
    and the list of rows from the dataframe

    if type_map is true, returns also a dictionary
    mapping each column name to the corresponding 
    postgres column type
    """
    df_2 = df.copy()
    df_2.reset_index(
        level=0, inplace=True
    )  # Make new column corresponding to the index, to save it too.
    df_2 = df_2.rename(str.lower, axis='columns')
    columns = list(df_2.columns.values)
    rows = [tuple(r) for r in df_2.values.astype(object)
            ]  # cast to type, to get only python types instead of numpy types
    if type_map:
        s = df_2.dtypes.map(lambda x: conversion_table.get(x.type, 'text'))
        return columns, rows, s.to_dict()
    return columns, rows