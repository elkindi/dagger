import numpy as np
import pandas as pd
import datetime as dt
from dbObject import DbObject
from dbConnection import connect, disconnect, get_engine


class DbResource(object):
    """docstring for DbResource"""
    def __enter__(self):
        self.db_interface_obj = DbInterface()
        self.db_interface_obj.connect()
        return self.db_interface_obj

    def __exit__(self, exc_type, exc_value, traceback):
        self.db_interface_obj.disconnect()


class DbInterface(object):
    """docstring for DbInterface"""
    scalar_classes = [int, float, str, bool]  # complex?
    array_like_classes = [list, tuple, set, frozenset, dict]
    date_like_classes = [dt.datetime, dt.date]
    pandas_classes = [pd.DataFrame, pd.Series]
    numpy_classes = [np.ndarray]

    def __init__(self):
        super(DbInterface, self).__init__()
        self.cur = None
        self.conn = None

    def __del__(self):
        if self.conn is not None:
            self.disconnect(False)

    def connect(self):
        self.cur, self.conn = connect()
        self.engine = get_engine()

    def disconnect(self, commit=True):
        disconnect(self.cur, self.conn)
        self.cur = None
        self.conn = None
        self.engine = None

    def commit(self):
        if self.conn is not None:
            self.conn.commit()

    def save(self, obj: DbObject):
        if obj.type in self.scalar_classes:
            self.save_scalar(obj)
        # elif obj.type in self.array_like_classes:
        #     self.save_array_like(obj)
        # elif obj.type in self.date_like_classes:
        #     self.save_date_like(obj)
        elif obj.type in self.pandas_classes:
            self.save_pandas(obj)
        elif obj.type in self.numpy_classes:
            self.save_numpy(obj)
        else:
            raise TypeError("Obj type is not handled yet")

    def save_scalar(self, obj: DbObject):
        sql = "INSERT INTO {}_scalar(t, lineno, name, value) VALUES (%s,%s,%s,%s)".format(
            obj.type.__name__)
        args = (obj.time, obj.lineno, obj.name, obj.value)
        self.cur.execute(sql, args)

    def save_pandas(self, obj: DbObject):
        sql = "INSERT INTO {}_object(t, lineno, name) VALUES (%s,%s,%s) RETURNING value".format(
            obj.type.__name__.lower())
        args = (obj.time, obj.lineno, obj.name)
        self.cur.execute(sql, args)
        table_id = self.cur.fetchone()[0]
        table_name = "{}_{}".format(obj.type.__name__.lower(), table_id)
        obj.value.to_sql(table_name, self.engine)

    def save_numpy(self, obj: DbObject):
        dim = len(obj.value.shape)
        if dim == 0 or dim == 1 or dim == 2:
            sql = "INSERT INTO np_{}d_object(t, lineno, name, dtype) VALUES (%s,%s,%s,%s) RETURNING value".format(
                dim)
            args = (obj.time, obj.lineno, obj.name, str(obj.value.dtype))
            self.cur.execute(sql, args)
            table_id = self.cur.fetchone()[0]
            table_name = "np_{}d_{}".format(dim, table_id)
            if dim < 2:
                pd_obj = pd.Series(data=obj.value, dtype=obj.value.dtype)
            else:
                pd_obj = pd.DataFrame(data=obj.value, dtype=obj.value.dtype)
            pd_obj.to_sql(table_name, self.engine)
        else:
            raise TypeError(
                "Numpy arrays of dimension higher than 2 are not handled yet")