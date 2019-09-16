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
        """
    return sql


# for 1d arrays (lists, tuples) of scalar values, 1 for each type and 1 for combined arrays
def prepare_scalar_1d_array_tables():
    sql = """
            CREATE TABLE IF NOT EXISTS int_scalar_1d_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value integer[] NOT NULL
            );
            CREATE TABLE IF NOT EXISTS float_scalar_1d_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value double precision[] NOT NULL
            );
            CREATE TABLE IF NOT EXISTS str_scalar_1d_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value text[] NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bool_scalar_1d_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value boolean[] NOT NULL
            );
            CREATE TABLE IF NOT EXISTS combined_scalar_1d_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                types text[] NOT NULL,
                value text[] NOT NULL
            );
        """
    return sql


# for 2d arrays (lists, tuples) of scalar values, 1 for each type and 1 for combined arrays
def prepare_scalar_1d_array_tables():
    sql = """
            CREATE TABLE IF NOT EXISTS int_scalar_2d_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value integer[][] NOT NULL
            );
            CREATE TABLE IF NOT EXISTS float_scalar_2d_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value double precision[][] NOT NULL
            );
            CREATE TABLE IF NOT EXISTS str_scalar_2d_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value text[][] NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bool_scalar_2d_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                value boolean[][] NOT NULL
            );
            CREATE TABLE IF NOT EXISTS combined_scalar_2d_array (
                id serial PRIMARY KEY,
                t timestamptz NOT NULL DEFAULT current_timestamp,
                lineno integer NOT NULL,
                name text NOT NULL,
                types text[][] NOT NULL,
                value text[][] NOT NULL
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