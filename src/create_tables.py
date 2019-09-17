import sys
from dbConnection import connect, disconnect


# for scalar values ex: 4, 'a', True, 12.90
def prepare_scalar_tables():
    sql = """
            CREATE TABLE IF NOT EXISTS int_scalar (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value integer NOT NULL
            );
            CREATE TABLE IF NOT EXISTS float_scalar (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value double precision NOT NULL
            );
            CREATE TABLE IF NOT EXISTS str_scalar (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value text NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bool_scalar (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value boolean NOT NULL
            );
            CREATE TABLE IF NOT EXISTS date_scalar (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value date NOT NULL
            );
            CREATE TABLE IF NOT EXISTS time_scalar (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value time NOT NULL
            );
            CREATE TABLE IF NOT EXISTS datetime_scalar (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value timestamp NOT NULL
            );
        """
    return sql


# for arrays (lists, tuples) of scalar values, 1 for each type and 1 for compound arrays
def prepare_scalar_array_tables():
    sql = """
            CREATE TABLE IF NOT EXISTS empty_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL
            );
            CREATE TABLE IF NOT EXISTS int_scalar_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value integer array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS float_scalar_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value double precision array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS str_scalar_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value text array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bool_scalar_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value boolean array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS date_scalar_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value date array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS time_scalar_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value time array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS datetime_scalar_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                value timestamp array NOT NULL
            );
            CREATE TABLE IF NOT EXISTS compound_scalar_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                arr_type text NOT NULL,
                name text NOT NULL,
                types text array NOT NULL,
                value text array NOT NULL
            );
        """
    return sql


def prepare_pandas_tables():
    sql = """
            CREATE TABLE IF NOT EXISTS dataframe_object (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value serial NOT NULL
            );
            CREATE TABLE IF NOT EXISTS series_object (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value serial NOT NULL
            );
        """
    return sql


def prepare_numpy_tables():
    sql = """
            CREATE TABLE IF NOT EXISTS np_0d_object (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value serial NOT NULL,
                dtype text NOT NULL
            );
            CREATE TABLE IF NOT EXISTS np_1d_object (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value serial NOT NULL,
                dtype text NOT NULL
            );
            CREATE TABLE IF NOT EXISTS np_2d_object (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value serial NOT NULL,
                dtype text NOT NULL
            );
        """
    return sql


def get_create_statements():
    sql_stmts = []
    sql_stmts.append(prepare_scalar_tables())
    sql_stmts.append(prepare_scalar_array_tables())
    sql_stmts.append(prepare_pandas_tables())
    sql_stmts.append(prepare_numpy_tables())
    return sql_stmts


def get_drop_statement():
    return """
            DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
        """


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


def drop_tables():
    try:
        (cur, conn) = connect()
    except Exception as e:
        print(e)
    else:
        sql = get_drop_statement()
        cur.execute(sql)
        disconnect(cur, conn)


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
    if arg == 'create':
        create_tables()
    elif arg == 'reset':
        reset_tables()
    elif arg == 'drop':
        drop_tables()


if __name__ == '__main__':
    main(sys.argv[1])