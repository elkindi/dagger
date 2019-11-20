import psycopg2
from config import config, engine_config
from sqlalchemy import create_engine


# Returns an sqlalchemy engine with the correct configs
def get_engine():
    eng_config = engine_config()
    print(eng_config)
    engine = create_engine(eng_config)
    return engine


# Connect to the database
# raise an exception if the connection fails
def connect():
    conn = None
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        return (cur, conn)
    except Exception as e:
        raise e


# disconnect from the database,
# and commit if needed
def disconnect(cur, conn, commit=True):
    cur.close()
    if commit:
        conn.commit()
    conn.close()