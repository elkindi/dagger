import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import create_engine
from config import config, engine_config


class DbInterface(object):

    scalar_classes = [int, float, str, bool, complex]
    array_like_classes = [list, tuple, set, frozenset, dict]
    date_like_classes = [dt.datetime, dt.date]
    pandas_classes = [pd.DataFrame, pd.Series]
    numpy_classes = [np.ndarray]

    def __init__(self, filename='../database.ini', section='postgresql'):
        super(DbInterface, self).__init__()
        self.db_config = config(filename, section)
        self.db_engine = create_engine(engine_config())
        self.prepare_tables()

    def prepare_tables(self):
        create_int_table = """
            CREATE TABLE IF NOT EXIST int_scalar (
                id integer PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value integer NOT NULL
            )
        """
        create_float_table = """
            CREATE TABLE IF NOT EXIST float_scalar (
                id integer PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value double precision NOT NULL
            )
        """
        create_str_table = """
            CREATE TABLE IF NOT EXIST str_scalar (
                id integer PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value text NOT NULL
            )
        """

    def save(self, obj):
        if type(obj) in self.scalar_classes:
            self.save_scalar(obj)
        elif type(obj) in self.array_like_classes:
            self.save_array_like(obj)
        elif type(obj) in self.date_like_classes:
            self.save_date_like(obj)
        elif type(obj) in self.pandas_classes:
            self.save_pandas(obj)
        elif type(obj) in self.numpy_classes:
            self.save_numpy(obj)
        else:
            raise TypeError("Obj type is not handled yet")

    def save_scalar(self, obj):
        if isinstance(obj, int):
            self.save_int(obj)
        elif isinstance(obj, float):
            self.save_float(obj)
        elif isinstance(obj, str):
            self.save_str(obj)
        elif isinstance(obj, bool):
            self.save_bool(obj)
        elif isinstance(obj, complex):
            self.save_complex(obj)

    def save_int(self, obj):
        if self.int_table == None:
            self.create_table('int')
            self.int_table = True
