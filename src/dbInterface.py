import numpy as np
import pandas as pd
from dbObject import DbObject
from psycopg2.extensions import AsIs
from datetime import date, time, datetime
from psycopg2.extras import execute_values
from dbConnection import connect, disconnect, get_engine
from array_utils import ArrayType, get_array_type, get_element_types


class DbResource(object):
    """
    DbResource class

    Allows to write:
    with DbResource() as db:
       db.save(...)

    The returned db object is an instance of DbInterface
    The connection and disconnection is handled automatically
    """
    def __enter__(self):
        self.db_interface_obj = DbInterface()
        self.db_interface_obj.connect()
        return self.db_interface_obj

    def __exit__(self, exc_type, exc_value, traceback):
        self.db_interface_obj.disconnect()


class DbInterface(object):
    """
    DbInterface class

    Interface to connect to the database and save DbObject instances
    Multiple python, numpy and pandas types are handled

    Connection and diconnection is manual
    For automatic connect and diconnect, use the DbResource class
    """
    scalar_classes = [int, float, str, bool, date, time, datetime]  # complex?
    array_like_classes = [list, tuple, set, frozenset]
    dict_class = [dict]  # Not handled yet
    pandas_classes = [pd.DataFrame, pd.Series]
    numpy_classes = [np.ndarray]

    # Used to convert a pandas column type into a postgres type
    # All other types are not handled specifically
    # and are converted to postgres text type
    conversion_table = {
        int: 'integer',
        float: 'double precision',
        str: 'text',
        bool: 'bool',
        date: 'date',
        time: 'time',
        datetime: 'timestamp',
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
        np.datetime64: 'timestamp',
        np.timedelta64: 'interval',
    }

    def __init__(self):
        super(DbInterface, self).__init__()
        self.cur = None
        self.conn = None

    # Disconnect without committing if the instance is deleted
    def __del__(self):
        if self.conn is not None:
            self.disconnect(False)

    # Connect to the database
    # raises an exception if the connection fails
    def connect(self):
        self.cur, self.conn = connect()
        self.engine = get_engine()

    # Diconnect to the database and commit the changes
    def disconnect(self, commit=True):
        disconnect(self.cur, self.conn, commit=commit)
        self.cur = None
        self.conn = None
        self.engine = None

    # Commit the changes (Could be called after each save call if needed)
    def commit(self):
        if self.conn is not None:
            self.conn.commit()

    # Save a DbObject in the database
    # If the type of the object is handled,
    # call the corresponding function
    # Otherwise, raise a TypeError
    def save(self, obj: DbObject):
        if obj.type in self.scalar_classes:
            self.save_scalar(obj)
        elif obj.type in self.array_like_classes:
            self.save_array_like(obj)
        # elif obj.type in self.dict_class:
        #     self.save_dict(obj)
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

    def save_array_like(self, obj: DbObject):
        try:
            arr_type = get_array_type(obj.value)
        except Exception as e:
            raise e
        else:
            sql, args = None, (obj.time, obj.lineno, type(obj.value).__name__,
                               obj.name)
            arr = list(obj.value)
            if arr_type is ArrayType.EMPTY:
                sql = "INSERT INTO empty_array(t, lineno, arr_type, name) VALUES (%s,%s,%s,%s)"
            elif arr_type is ArrayType.COMPOUND:
                sql = "INSERT INTO compound_scalar_array(t, lineno, arr_type, name, types, value) VALUES (%s,%s,%s,%s,%s,%s)"
                args += (get_element_types(arr), list(map(str, arr)))
            else:
                sql = "INSERT INTO {}_scalar_array(t, lineno, arr_type, name, value) VALUES (%s,%s,%s,%s,%s)".format(
                    type(arr[0]).__name__)
                args += (arr, )
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