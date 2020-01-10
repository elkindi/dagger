import sys
from dbConnection import connect, disconnect
"""
Code for creating and resetting the database tables

Most tables have the same columns, but the tables themselves
indicate what the type of their elements is

This could maybe be changed by creating one table with an extra column
specifying the object type instead of all the 

Not sure which idea is better
"""


# Possibly save every variable in this table
# and only have ref_id and value in other tables
def prepare_main_table():
    sql = """
            CREATE TABLE IF NOT EXISTS variable (
                id serial PRIMARY KEY,
                b_id integer, # Block id
                s_id integer, # Split id
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                type text NOT NULL, # Type of the object, specifies in which table the value is stored
                val_id # Id of the value in the corresponding 'type' table
            );
        """


# for scalar values ex: 4, 'a', True, 12.90
# and for date, time and datetime types
# (they are handled as scalar values)
def prepare_scalar_tables():
    sql = """
            CREATE TABLE IF NOT EXISTS int_scalar (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value integer NOT NULL
            );
            CREATE TABLE IF NOT EXISTS float_scalar (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value double precision NOT NULL
            );
            CREATE TABLE IF NOT EXISTS str_scalar (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value text NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bool_scalar (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value boolean NOT NULL
            );
            CREATE TABLE IF NOT EXISTS date_scalar (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value date NOT NULL
            );
            CREATE TABLE IF NOT EXISTS time_scalar (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value time NOT NULL
            );
            CREATE TABLE IF NOT EXISTS datetime_scalar (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value timestamp NOT NULL
            );
        """
    return sql


# for arrays (lists, tuples, sets and frozensets) of scalar values,
# 1 for each type and 1 for compound arrays
def prepare_scalar_array_tables():
    sql = """
            CREATE TABLE IF NOT EXISTS empty_array (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL
            );
            CREATE TABLE IF NOT EXISTS int_scalar_array (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value integer array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS float_scalar_array (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value double precision array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS str_scalar_array (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value text array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bool_scalar_array (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value boolean array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS date_scalar_array (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value date array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS time_scalar_array (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value time array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS datetime_scalar_array (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value timestamp array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS compound_scalar_array (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                types text array NOT NULL,
                value text array NOT NULL
            );
        """
    return sql


# Pandas dataframes and series
def prepare_pandas_tables():
    sql = """
            CREATE TABLE IF NOT EXISTS dataframe_object (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value serial NOT NULL
            );
            CREATE TABLE IF NOT EXISTS series_object (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value serial NOT NULL
            );
            CREATE TABLE IF NOT EXISTS dataframe_delta_object (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                rlist integer array NOT NULL,
                clist text array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS dataframe_data (
                rid serial PRIMARY KEY,
                index bigint NOT NULL
            );
            CREATE TABLE IF NOT EXISTS dataframe_max_rid (
                index bigint PRIMARY KEY,
                max_rid integer NOT NULL
            );
        """
    return sql


# Numpy arrays for 0d, 1d and 2d arrays
def prepare_numpy_tables():
    sql = """
            CREATE TABLE IF NOT EXISTS np_0d_object (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value serial NOT NULL,
                dtype text NOT NULL
            );
            CREATE TABLE IF NOT EXISTS np_1d_object (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value serial NOT NULL,
                dtype text NOT NULL
            );
            CREATE TABLE IF NOT EXISTS np_2d_object (
                id serial PRIMARY KEY,
                b_id integer,
                s_id integer,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value serial NOT NULL,
                dtype text NOT NULL
            );
        """
    return sql


# Tables used for testing
def prepare_test_tables():
    sql = """
            CREATE TABLE IF NOT EXISTS test_dataframe_object (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                rlist integer array NOT NULL,
                clist text array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS test_dataframe_table (
                rid serial PRIMARY KEY,
                index bigint NOT NULL
            );
            CREATE TABLE IF NOT EXISTS test_mrm_table (
                index bigint PRIMARY KEY,
                max_rid integer NOT NULL
            );
        """
    return sql


# List of all needed statements to fully create the database
def get_create_statements():
    sql_stmts = []
    sql_stmts.append(prepare_scalar_tables())
    sql_stmts.append(prepare_scalar_array_tables())
    sql_stmts.append(prepare_pandas_tables())
    sql_stmts.append(prepare_numpy_tables())
    sql_stmts.append(prepare_test_tables())
    return sql_stmts


# Drop all tables in the database
# used to reset the database
def get_drop_statement():
    return """
            DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
        """


# Create the database tables
def create_tables():
    try:
        (cur, conn) = connect()
    except Exception as e:
        print(e)
    else:
        sql_stmts = get_create_statements()
        for sql in sql_stmts:
            cur.execute(sql)
        disconnect(cur, conn)


# Drop the database tables
def drop_tables():
    try:
        (cur, conn) = connect()
    except Exception as e:
        print(e)
    else:
        sql = get_drop_statement()
        cur.execute(sql)
        disconnect(cur, conn)


# Reset the database
# First drop, then recreate all the tables
def reset_tables():
    try:
        (cur, conn) = connect()
    except Exception as e:
        print(e)
    else:
        sql_stmts = [get_drop_statement()]
        sql_stmts.extend(get_create_statements())
        for sql in sql_stmts:
            cur.execute(sql)
        disconnect(cur, conn)


def main(arg):
    if arg == 'create' or arg == '-c':
        print("Creating tables")
        create_tables()
    elif arg == 'reset' or arg == '-r':
        print("Resetting tables")
        reset_tables()
    elif arg == 'drop' or arg == '-d':
        print("Dropping tables")
        drop_tables()
    else:
        print("The given argument was incorrect\n" + "Options are:\n" +
              " - 'create' or '-c' to create the tables,\n" +
              " - 'drop' or '-d' to drop all the tables,\n" +
              " - 'reset' or '-r' to reset the tables")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(
            "Please add an argument to specify what to do with the database tables\n"
            + "Options are:\n" +
            " - 'create' or '-c' to create the tables,\n" +
            " - 'drop' or '-d' to drop all the tables,\n" +
            " - 'reset' or '-r' to reset the tables")
    elif len(sys.argv) > 2:
        print(
            "Please use only one argument to specify what to do with the database tables\n"
            + "Options are:\n" +
            " - 'create' or '-c' to create the tables,\n" +
            " - 'drop' or '-d' to drop all the tables,\n" +
            " - 'reset' or '-r' to reset the tables")
    else:
        main(sys.argv[1])