import os
import sys
import sqlite3
import pandas as pd


def add_lookup(data_df, adm_level, conn):

    field_list = level_lkp(adm_level, include_adm_fields=True)
    field_text = ', '.join(field_list)

    lkp_df = pd.read_sql('SELECT {0} FROM adm_lkp GROUP BY {0}'.format(field_text), conn)

    join_lkp = {0: ['iso'], 1: ['iso', 'adm1'], 2: ['iso', 'adm1', 'adm2']}
    join_fields = join_lkp[adm_level]

    data_df = pd.merge(data_df, lkp_df, on=join_fields, how='left')

    return data_df


def db_connect():

    utilities_dir = os.path.dirname(os.path.realpath(__file__))
    data_db = os.path.join(os.path.dirname(utilities_dir), 'data.db')
    conn = sqlite3.connect(data_db)

    return conn


def level_lkp(adm_level, include_adm_fields=False):

    if include_adm_fields:
        level_dict = {0: ['iso', 'name0'], 1: ['iso', 'adm1', 'name0', 'name1'],
                      2: ['iso', 'adm1', 'adm2', 'name0', 'name1', 'name2']}

    else:
        level_dict = {0: ['iso'], 1: ['iso', 'adm1'], 2: ['iso', 'adm1', 'adm2']}

    return level_dict[adm_level]


def country_text_lookup(adm_level):

    country_text_dict = {0: "df.name0", 1: "df.name0 + '_' + df.name1",
                         2: "df.name0 + '_' + df.name1 + '_' + df.name2"}

    return country_text_dict[adm_level]


def validate_input_data(root_dir):

    data_db = os.path.join(root_dir, 'data.db')

    if not os.path.exists(data_db):
        print 'data.db not found in the root directory'
        print 'Please run load_data.py to create this database from source files'
        sys.exit()

    conn = db_connect()
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

    table_list = [x[0] for x in cursor.fetchall()]

    for tbl in ['loss', 'extent', 'gain', 'adm_lkp']:
        if tbl not in table_list:
            raise IndexError('Table {} must be in data.db for this process to work; run load_data.py to create it')
