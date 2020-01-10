import numpy as np
import pandas as pd
from dbObject import DbObject
from psycopg2.extensions import AsIs, ISOLATION_LEVEL_AUTOCOMMIT
from datetime import date, time, datetime
from psycopg2.extras import execute_values
from dataframe_utils import get_df_cols_rows
from database_utils import get_db_cols, add_columns
from dbConnection import connect, disconnect, get_engine
from psycopg2.extras import execute_values, execute_batch
from array_utils import ArrayType, get_array_type, get_element_types


class DbResource(object):
    """
    DbResource class

    Allows to write:
    db_resource = DbResource(delta_logging)
    with db_resource as db:
       db.save(...)

    The returned db object is an instance of DbInterface
    The connection and disconnection is handled automatically
    """
    def __init__(self, delta_logging=True):
        self.db_interface_obj = DbInterface(delta_logging)

    def __enter__(self):
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

    def __init__(self, delta_logging=True):
        super(DbInterface, self).__init__()
        self.delta_logging = delta_logging
        self.cur = None
        self.conn = None
        self.block_id = None
        self.split_id = None

    # Disconnect without committing if the instance is deleted
    def __del__(self):
        if self.conn is not None:
            self.disconnect(False)

    # when deepcopying the globals,
    # just return the same dbInterface instance
    def __deepcopy__(self, memo):
        return self

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

    # Vacuum the database
    # To do after a big table update (ex: after
    # adding a new column with data during delta logging)
    def vacuum(self):
        self.commit()
        old_isolation_level = self.conn.isolation_level
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cur.execute("VACUUM FULL")
        self.commit()
        self.conn.set_isolation_level(old_isolation_level)

    # Set the current split id
    def set_block_id(self, block_id: int):
        if isinstance(block_id, int):
            if block_id >= 0:
                self.block_id = block_id
            else:
                raise ValueError("block_id must be positive")
        else:
            raise TypeError("block_id must be an integer")

    # Set the current split id to None
    def reset_block_id(self):
        self.block_id = None

    # Set the current split id
    def set_split_id(self, split_id: int):
        if isinstance(split_id, int):
            if split_id >= 0:
                self.split_id = split_id
            else:
                raise ValueError("split_id must be positive")
        else:
            raise TypeError("split_id must be an integer")

    # Set the current split id to None
    def reset_split_id(self):
        self.split_id = None

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
        sql = """
            INSERT INTO {}_scalar(b_id, s_id, t, lineno, name, value) 
            VALUES (%s,%s,%s,%s,%s,%s)""".format(obj.type.__name__)
        args = (self.block_id, self.split_id, obj.time, obj.lineno, obj.name, obj.value)
        self.cur.execute(sql, args)

    def save_array_like(self, obj: DbObject):
        try:
            arr_type = get_array_type(obj.value)
        except Exception as e:
            raise e
        else:
            sql, args = None, (self.block_id, self.split_id, obj.time, obj.lineno,
                               type(obj.value).__name__, obj.name)
            arr = list(obj.value)
            if arr_type is ArrayType.EMPTY:
                sql = """
                    INSERT INTO empty_array(b_id, s_id, t, lineno, arr_type, name) 
                    VALUES (%s,%s,%s,%s,%s,%s)"""
            elif arr_type is ArrayType.COMPOUND:
                sql = """
                    INSERT INTO compound_scalar_array(b_id, s_id, t, lineno, arr_type, name, types, value) 
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
                args += (get_element_types(arr), list(map(str, arr)))
            else:
                sql = """
                    INSERT INTO {}_scalar_array(b_id, s_id, t, lineno, arr_type, name, value) 
                    VALUES (%s,%s,%s,%s,%s,%s,%s)""".format(
                    type(arr[0]).__name__)
                args += (arr, )
            self.cur.execute(sql, args)

    def save_pandas(self, obj: DbObject):
        if obj.type is pd.Series:
            self.save_pandas_default(obj)
        elif obj.type is pd.DataFrame:
            if self.delta_logging:
                self.save_dataframe_delta(obj)
                self.vacuum()
            else:
                self.save_pandas_default(obj)
        else:
            raise TypeError("This should not happen")

    def save_pandas_default(self, obj: DbObject):
        sql = """
            INSERT INTO {}_object(b_id, s_id, t, lineno, name) 
            VALUES (%s,%s,%s,%s,%s) RETURNING value""".format(
            obj.type.__name__.lower())
        args = (self.block_id, self.split_id, obj.time, obj.lineno, obj.name)
        self.cur.execute(sql, args)
        table_id = self.cur.fetchone()[0]
        table_name = "{}_{}".format(obj.type.__name__.lower(), table_id)
        obj.value.to_sql(table_name, self.engine)

    def save_dataframe_delta(self, obj: DbObject):
        """
        Dataframe delta saving function

        Adds new and updated data to the dataframe_data table
        Saved the set of rows and columns of that table 
        that are needed to recreate the dataframe

        Assumptions:
            - The new dataframe is a modified version of the previous dataframe
              (i.e. we suppose iterative modifications to the dataframe)
            - Columns of the dataframe cannot change type,
              create a new column instead of modifying the type
            - The indexes of the dataframe are integers and we cannot reuse old indexes
              that were removed

        For more explanation of the execution of the function,
        please take a look at the file 'save_df_test.py' containing
        the iterative developement of this function and explanations of its execution
        """
        df = obj.value
        db_cols, db_type_map = get_db_cols(self.cur, 'dataframe_data')
        df_cols, df_rows, df_type_map = get_df_cols_rows(df)
        wrong = [
            k for k in db_type_map
            if k in df_type_map and db_type_map[k] != df_type_map[k]
        ]
        if len(wrong) != 0:
            raise ValueError(
                """A column type was changed, please don't do this. 
                Create a new column instead. (previous: {}, found: {})""".
                format(db_type_map[wrong[0]], df_type_map[wrong[0]]))

        inter_cols = [col for col in df_cols if col in db_cols]
        add_cols = [col for col in df_cols if col not in db_cols]
        df_indices = tuple([r[0] for r in df_rows])

        max_rid_sql = "SELECT max_rid FROM dataframe_max_rid WHERE index in %s"
        self.cur.execute(max_rid_sql, (df_indices, ))
        max_rid_list = tuple([r[0] for r in self.cur])

        if len(max_rid_list) > 0:
            db_data_sql = """SELECT rid,{} 
                    FROM dataframe_data 
                    WHERE rid in %s
                    """.format(','.join(inter_cols))
            self.cur.execute(db_data_sql, (max_rid_list, ))
            r_list = [r for r in self.cur]
        else:
            r_list = []
        hash_list = [(hash(r[1:]), r[0]) for r in r_list]
        hash_dic = dict(hash_list)

        rids = []
        update_rows = []
        new_rows = []
        for i, r in enumerate(df_rows):
            row = tuple([
                self.cast(r[0], df_type_map.get(r[1]))
                for r in zip(r, df_cols) if r[1] in db_cols
            ])
            # if i < 5:
            #     print(row)
            rest_row = tuple(
                [r[0] for r in zip(r, df_cols) if r[1] not in db_cols])
            h = hash(row)
            if h in hash_dic:
                rid = hash_dic.get(h)
                update_rows.append((*rest_row, rid))
                rids.append(rid)
            else:
                new_rows.append(r)

        if len(add_cols) != 0:
            add_columns(self.cur, 'dataframe_data', add_cols, df_type_map)
            update_sql = """
                UPDATE dataframe_data 
                SET {} WHERE rid = %s""".format(','.join(
                map(lambda x: '{} = %s'.format(x), add_cols)))
            execute_batch(self.cur, update_sql, update_rows)

        insert_sql = """
            INSERT INTO dataframe_data({})
            VALUES %s RETURNING rid, index
        """.format(','.join(df_cols))
        new_rids_indices = execute_values(self.cur,
                                          insert_sql,
                                          new_rows,
                                          fetch=True)
        update_max_rids_sql = """
            INSERT INTO dataframe_max_rid(max_rid, index) 
            VALUES %s 
            ON CONFLICT (index) DO UPDATE SET max_rid = EXCLUDED.max_rid"""
        execute_values(self.cur, update_max_rids_sql, new_rids_indices)
        new_rids = [x[0] for x in new_rids_indices]
        rids.extend(new_rids)

        insert_versioning_sql = """
            INSERT INTO dataframe_delta_object(b_id, s_id, t, lineno, name, rlist, clist) 
            VALUES (%s,%s,%s,%s,%s,%s,%s)"""
        args = (self.block_id, self.split_id, obj.time, obj.lineno, obj.name, rids, df_cols)
        self.cur.execute(insert_versioning_sql, args)

    def save_numpy(self, obj: DbObject):
        dim = len(obj.value.shape)
        if dim == 0 or dim == 1 or dim == 2:
            sql = """
                INSERT INTO np_{}d_object(b_id, s_id, t, lineno, name, dtype) 
                VALUES (%s,%s,%s,%s,%s,%s) RETURNING value""".format(dim)
            args = (self.block_id, self.split_id, obj.time, obj.lineno, obj.name,
                    str(obj.value.dtype))
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

    def cast(self, elem, psql_type):
        """
        Casts the element to the correct type/format
        depending on its equivalent postgres type

        This is used to be able to compare new (from the new dataframe) 
        and old (from the database) dataframe rows using their hashes
        """
        if psql_type == 'real':
            return float(format(elem, '.6g'))
        elif psql_type == 'double precision':
            return float(format(elem, '.15g'))
        elif psql_type == 'timestamp':
            if isinstance(elem, pd.Timestamp):
                return elem.to_pydatetime()
            else:
                return elem
        elif psql_type == 'text':
            if type(elem) == float:
                return "NaN"
            return str(elem)
        else:
            return elem