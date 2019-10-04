import pandas as pd
from time import clock
from database_utils import get_db_cols
from dataframe_utils import get_df_cols_rows
from psycopg2.extras import execute_values, execute_batch

# Used during the testing of the execution duration of the functions
# Do not show durations that are longer than this value in seconds
DT_LIMIT = 0.1


def cast(elem, psql_type):
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


def save_df(obj, cur):
    """
    Third df delta saving function
    Improvements:
        - faster addition of the data in the new columns
          using execute_batch
    Issues:
        - need to clean db (VACUUM FULL;) to release empty space 
          created by the addition of the new columns
    """

    df = obj.value

    t_start = clock()

    t1 = clock()

    db_cols, db_type_map = get_db_cols(cur, 'test_dataframe_table')
    # print(db_cols)
    # print(db_type_map)

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Get db specs: ", t2 - t1)

    t1 = clock()

    df_cols, df_rows, df_type_map = get_df_cols_rows(df)
    # print(df_cols)
    # print(df_type_map)

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Get df specs: ", t2 - t1)

    t1 = clock()

    shared_items = [
        k for k in db_type_map
        if k in df_type_map and db_type_map[k] != df_type_map[k]
    ]
    if len(shared_items) != 0:
        print("A column type was changed, please don't do this.")
        return

    inter_cols = [col for col in df_cols if col in db_cols]
    # print(inter_cols)
    add_cols = [col for col in df_cols if col not in db_cols]
    # print(add_cols)

    df_indices = tuple([r[0] for r in df_rows])

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Inter_cols, add_cols and indices: ", t2 - t1)

    t1 = clock()

    max_rid_sql = "SELECT max_rid FROM test_mrm_table WHERE index in %s"
    cur.execute(max_rid_sql, (df_indices, ))
    max_rid_list = tuple([r[0] for r in cur])

    if len(max_rid_list) > 0:
        db_data_sql = """SELECT rid,{} 
                FROM test_dataframe_table 
                WHERE rid in %s
                """.format(','.join(inter_cols))
        cur.execute(db_data_sql, (max_rid_list, ))
        r_list = [r for r in cur]
    else:
        r_list = []

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Get data from db table: ", t2 - t1)

    t1 = clock()

    hash_list = [(hash(r[1:]), r[0]) for r in r_list]
    hash_dic = dict(hash_list)

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Hash dic: ", t2 - t1)

    t1 = clock()

    rids = []
    update_rows = []
    new_rows = []
    for i, r in enumerate(df_rows):
        row = tuple([
            cast(r[0], df_type_map.get(r[1])) for r in zip(r, df_cols)
            if r[1] in db_cols
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

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Compute update_rows and new_rows: ", t2 - t1)

    t1 = clock()

    if len(add_cols) != 0:
        alter_sql = "ALTER TABLE test_dataframe_table {};".format(','.join(
            map(lambda x: 'ADD COLUMN {} {}'.format(x, df_type_map.get(x)),
                add_cols)))
        cur.execute(alter_sql)
        update_sql = "UPDATE test_dataframe_table SET {} WHERE rid = %s".format(
            ','.join(map(lambda x: '{} = %s'.format(x), add_cols)))
        execute_batch(cur, update_sql, update_rows)

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Alter table and update: ", t2 - t1)

    t1 = clock()

    insert_sql = """
        INSERT INTO test_dataframe_table({})
        VALUES %s RETURNING rid, index
    """.format(','.join(df_cols))
    new_rids_indices = execute_values(cur, insert_sql, new_rows, fetch=True)
    update_max_rids_sql = """
        INSERT INTO test_mrm_table(max_rid, index) 
        VALUES %s 
        ON CONFLICT (index) DO UPDATE SET max_rid = EXCLUDED.max_rid"""
    execute_values(cur, update_max_rids_sql, new_rids_indices)
    new_rids = [x[0] for x in new_rids_indices]
    print(len(new_rids))
    rids.extend(new_rids)

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Insert new tuples: ", t2 - t1)

    t1 = clock()

    insert_versioning_sql = """
        INSERT INTO test_dataframe_object(t, lineno, name, rlist, clist) 
        VALUES (%s, %s, %s, %s, %s)"""
    args = (obj.time, obj.lineno, obj.name, rids, df_cols)
    cur.execute(insert_versioning_sql, args)

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Save version: ", t2 - t1)

    t_end = clock()
    print("Total time: ", t_end - t_start)


def save_df_2(obj, cur):
    """
    Second df delta saving function
    Improvements:
        - faster data retrieval from the database
        - uses a new table that does the job 
          of the inner select statement from the last version
    Issues:
        - adding new columns with data is still slow
        - need to clean db (VACUUM FULL;) to release empty space 
          created by the addition of the new columns
    """

    df = obj.value

    t_start = clock()

    t1 = clock()

    db_cols, db_type_map = get_db_cols(cur, 'test_dataframe_table')
    # print(db_cols)
    # print(db_type_map)

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Get db specs: ", t2 - t1)

    t1 = clock()

    df_cols, df_rows, df_type_map = get_df_cols_rows(df)
    # print(df_cols)
    # print(df_type_map)

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Get df specs: ", t2 - t1)

    t1 = clock()

    shared_items = [
        k for k in db_type_map
        if k in df_type_map and db_type_map[k] != df_type_map[k]
    ]
    if len(shared_items) != 0:
        print("A column type was changed, please don't do this.")
        return

    inter_cols = [col for col in df_cols if col in db_cols]
    # print(inter_cols)
    add_cols = [col for col in df_cols if col not in db_cols]
    # print(add_cols)

    df_indices = tuple([r[0] for r in df_rows])

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Inter_cols, add_cols and indices: ", t2 - t1)

    t1 = clock()

    max_rid_sql = "SELECT max_rid FROM test_mrm_table WHERE index in %s"
    cur.execute(max_rid_sql, (df_indices, ))
    max_rid_list = tuple([r[0] for r in cur])

    if len(max_rid_list) > 0:
        db_data_sql = """SELECT rid,{} 
                FROM test_dataframe_table 
                WHERE rid in %s
                """.format(','.join(inter_cols))
        cur.execute(db_data_sql, (max_rid_list, ))
        r_list = [r for r in cur]
    else:
        r_list = []

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Get data from db table: ", t2 - t1)

    t1 = clock()

    hash_list = [(hash(r[1:]), r[0]) for r in r_list]
    hash_dic = dict(hash_list)

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Hash dic: ", t2 - t1)

    t1 = clock()

    rids = []
    update_rows = []
    new_rows = []
    for i, r in enumerate(df_rows):
        row = tuple([
            cast(r[0], df_type_map.get(r[1])) for r in zip(r, df_cols)
            if r[1] in db_cols
        ])
        # if i < 5:
        #     print(row)
        rest_row = tuple(
            [r[0] for r in zip(r, df_cols) if r[1] not in db_cols])
        h = hash(row)
        if h in hash_dic:
            rid = hash_dic.get(h)
            update_rows.append((rid, rest_row))
            rids.append(rid)
        else:
            new_rows.append(r)

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Compute update_rows and new_rows: ", t2 - t1)

    t1 = clock()

    if len(add_cols) != 0:
        alter_sql = "ALTER TABLE test_dataframe_table {};".format(','.join(
            map(lambda x: 'ADD COLUMN {} {}'.format(x, df_type_map.get(x)),
                add_cols)))
        cur.execute(alter_sql)
        update_sql = "UPDATE test_dataframe_table SET {} WHERE rid = %s".format(
            ','.join(map(lambda x: '{} = %s'.format(x), add_cols)))
        for (rid, rest_row) in update_rows:
            cur.execute(update_sql, (*rest_row, rid))

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Alter table and update: ", t2 - t1)

    t1 = clock()

    insert_sql = """
        INSERT INTO test_dataframe_table({})
        VALUES %s RETURNING rid, index
    """.format(','.join(df_cols))
    new_rids_indices = execute_values(cur, insert_sql, new_rows, fetch=True)
    update_max_rids_sql = """
        INSERT INTO test_mrm_table(max_rid, index) 
        VALUES %s 
        ON CONFLICT (index) DO UPDATE SET max_rid = EXCLUDED.max_rid"""
    execute_values(cur, update_max_rids_sql, new_rids_indices)
    new_rids = [x[0] for x in new_rids_indices]
    print(len(new_rids))
    rids.extend(new_rids)

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Insert new tuples: ", t2 - t1)

    t1 = clock()

    insert_versioning_sql = """
        INSERT INTO test_dataframe_object(t, lineno, name, rlist, clist) 
        VALUES (%s, %s, %s, %s, %s)"""
    args = (obj.time, obj.lineno, obj.name, rids, df_cols)
    cur.execute(insert_versioning_sql, args)

    t2 = clock()
    if t2 - t1 > DT_LIMIT:
        print("Save version: ", t2 - t1)

    t_end = clock()
    print("Total time: ", t_end - t_start)


def save_df_1(obj, cur):
    """
    First df delta saving function
    How it works:
        - get columns and type of each column from the data table in the database
        - get columns and type of each column from the new dataframe
        - compute intersection and difference
        - get the list of indexes in the new dataframe
        - get the correct rows and columns from the database
        - compute a hash dictionary of the retrieved rows
        - for each row of the new dataframe, 
          check if they have to be updated (new columns added)
          or if they have to be inserted (new row added or value of old column was updated)
        - add new columns with corresponding values
        - add updated or new rows
        - save set of row ids and column names in the dataframe delta object table
    Issues:
        - getting data from the database is slow
        - adding new columns with data is slow
        - need to clean db (VACUUM FULL;) to release empty space 
          created by the addition of the new columns
    """

    df = obj.value

    t1 = clock()

    db_cols, db_type_map = get_db_cols(cur, 'test_dataframe_table')
    # print(db_cols)
    # print(db_type_map)

    t2 = clock()
    print("Get db specs: ", t2 - t1)

    t1 = clock()

    df_cols, df_rows, df_type_map = get_df_cols_rows(df)
    # print(df_cols)
    # print(df_type_map)

    t2 = clock()
    print("Get df specs: ", t2 - t1)

    t1 = clock()

    shared_items = [
        k for k in db_type_map
        if k in df_type_map and db_type_map[k] != df_type_map[k]
    ]
    if len(shared_items) != 0:
        print("A column type was changed, please don't do this.")
        return

    inter_cols = [col for col in df_cols if col in db_cols]
    # print(inter_cols)
    add_cols = [col for col in df_cols if col not in db_cols]
    # print(add_cols)

    df_indices = tuple([r[0] for r in df_rows])

    t2 = clock()
    print("Inter_cols, add_cols and indices: ", t2 - t1)

    t1 = clock()

    sql = """SELECT rid,{}
             FROM test_dataframe_table
             WHERE index in %s
             AND rid = (
                SELECT max(t2.rid)
                FROM test_dataframe_table AS t2
                WHERE t2.index = test_dataframe_table.index)
            """.format(','.join(inter_cols))
    cur.execute(sql, (df_indices, ))
    r_list = [r for r in cur]

    t2 = clock()
    print("Get data from db table: ", t2 - t1)

    t1 = clock()

    hash_list = [(hash(r[1:]), r[0]) for r in r_list]
    hash_dic = dict(hash_list)

    t2 = clock()
    print("Hash dic: ", t2 - t1)

    t1 = clock()

    rids = []
    update_rows = []
    new_rows = []
    for i, r in enumerate(df_rows):
        row = tuple([
            cast(r[0], df_type_map.get(r[1])) for r in zip(r, df_cols)
            if r[1] in db_cols
        ])
        # if i < 5:
        #     print(row)
        rest_row = tuple(
            [r[0] for r in zip(r, df_cols) if r[1] not in db_cols])
        h = hash(row)
        if h in hash_dic:
            rid = hash_dic.get(h)
            update_rows.append((rid, rest_row))
            rids.append(rid)
        else:
            new_rows.append(r)

    t2 = clock()
    print("Compute update_rows and new_rows: ", t2 - t1)

    t1 = clock()

    if len(add_cols) != 0:
        alter_sql = "ALTER TABLE test_dataframe_table {};".format(','.join(
            map(lambda x: 'ADD COLUMN {} {}'.format(x, df_type_map.get(x)),
                add_cols)))
        cur.execute(alter_sql)
        update_sql = "UPDATE test_dataframe_table SET {} WHERE rid = %s".format(
            ','.join(map(lambda x: '{} = %s'.format(x), add_cols)))
        for (rid, rest_row) in update_rows:
            cur.execute(update_sql, (*rest_row, rid))

    t2 = clock()
    print("Alter table and update: ", t2 - t1)

    t1 = clock()

    insert_sql = """
        INSERT INTO test_dataframe_table({})
        VALUES %s RETURNING rid
    """.format(','.join(df_cols))
    new_rids = execute_values(cur, insert_sql, new_rows, fetch=True)
    new_rids = [x[0] for x in new_rids]
    print(len(new_rids))
    rids.extend(new_rids)

    t2 = clock()
    print("Insert new tuples: ", t2 - t1)

    t1 = clock()

    insert_versioning_sql = "INSERT INTO test_dataframe_object(t, lineno, name, rlist, clist) VALUES (%s, %s, %s, %s, %s)"
    args = (obj.time, obj.lineno, obj.name, rids, df_cols)
    cur.execute(insert_versioning_sql, args)

    t2 = clock()
    print("Save version: ", t2 - t1)