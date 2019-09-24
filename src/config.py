from configparser import ConfigParser

DATABASE_CONFIG_PATH = '../database.ini'


# Get the database config from the config file
# Returns a dictionary with the configs
def config(filename=DATABASE_CONFIG_PATH, section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(
            section, filename))

    return db


# Get the configs in the format needed to create an sqlalchemy engine
def engine_config(filename=DATABASE_CONFIG_PATH, section='postgresql'):
    db = config(filename, section)
    return 'postgresql+psycopg2://{}@{}:{}/{}'.format(db['user'], db['host'],
                                                      db['port'],
                                                      db['database'])