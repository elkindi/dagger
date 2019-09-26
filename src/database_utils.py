def get_db_cols(cur, table_name, schema='public', type_map=True):
    """
    Gets the column names of a given table

    if type_map is true, returns also a dictionary
    mapping each column name to the corresponding 
    postgres column type
    """
    db_cols_sql = """SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = '{}' 
        AND table_name   = '{}';
    """.format(schema, table_name)
    cur.execute(db_cols_sql)
    res_rows = [row for row in cur][1:]
    cols = [row[0] for row in res_rows]
    if type_map:
        return cols, dict(res_rows)
    return cols


def add_columns(cur, table_name, new_columns, type_map):
    """
    Add new columns to a database table

    'type_map' is a dictionary mapping the columns name 
    to the corresponding postgres column type
    """
    alter_sql = "ALTER TABLE {} {};".format(
        table_name, ','.join(
            map(lambda x: 'ADD COLUMN {} {}'.format(x, type_map.get(x)),
                new_columns)))
    cur.execute(alter_sql)
