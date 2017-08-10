#!/usr/bin/env python
#!/bin/env python
import os
import os.path
import M6.Common.Default as Default
from M6.Common.DB import Backend
import types
import sys

import hashlib
import M6.Common.Default as Default

from M6.Common.Table.TableOption.Definition import TableOptionType

PRAGMA_COMMAND = []
PRAGMA_COMMAND.append("PRAGMA page_size = 16384;")
PRAGMA_COMMAND.append("PRAGMA auto_vacuum = 1;")


CREATE_TABLE_QUERY = {}
CREATE_TABLE_QUERY['SYS_TABLE_LOCATION'] = """
create table SYS_TABLE_LOCATION (
    TABLE_KEY        TEXT NOT NULL,
    TABLE_PARTITION  TEXT NOT NULL,
    NODE_ID          INTEGER NOT NULL,
    STATUS         TEXT,

    PRIMARY KEY ( TABLE_KEY, TABLE_PARTITION, NODE_ID )
);
"""

CREATE_TABLE_QUERY['SYS_SSD_INFO'] = """
create table SYS_SSD_INFO (
    NODE_ID NUMBER,
    P_NAME TEXT,
    P_SIZE_T NUMBER,
    P_SIZE_U NUMBER,
    UPDATETIME NUMBER
);
"""

CREATE_INDEX_QUERY = {}
CREATE_INDEX_QUERY['SYS_TABLE_LOCATION'] = """
CREATE INDEX sys_table_location_idx on SYS_TABLE_LOCATION (TABLE_PARTITION, TABLE_KEY);
"""

SYSTEM_QUERY = {}
SYSTEM_QUERY['INSERT_TABLE_INFO_STRING_LOCAL'] = """
    INSERT INTO SYS_TABLE_INFO (
        TABLE_NAME,
        SCOPE,
        SQL_SCRIPT,
        RAM_EXP_TIME,
        SSD_EXP_TIME,
        DSK_EXP_TIME,
        KEY_STRING,
        PARTITION_RANGE,
        PARTITION_STRING
    )
    VALUES ('%s', '%s', '%s', %d, %d, %d, '%s', %d, '%s');"""
SYSTEM_QUERY['INSERT_TABLE_PRIV_STRING'] = """
    INSERT INTO SYS_TABLE_PRIV (
        TABLE_NAME,
        USER,
        SELECT_PRIV,
        INSERT_PRIV,
        UPDATE_PRIV,
        DELETE_PRIV,
        DROP_PRIV, ALTER_PRIV
    )
    VALUES ('%s', '*', %d, %d, %d, %d, %d, %d);"""

def _execute_query(table_name, query_set):
    file_path = "%s/%s.DAT" % (Default.M6_MASTER_DATA_DIR, table_name)
    bd = Backend([file_path])
    cur = bd.GetConnection().cursor()
    for item in query_set:
        if item == "PRAGMA":
            for cmd in PRAGMA_COMMAND:
                cur.execute(cmd)
        else:
            if type(item) == types.StringType:
                cur.execute(item)
            else:
                for q in item:
                    cur.execute(q)
    bd.Disconnect()

def _create_sys_table_location(table_name):
    print '    !!! SYS_TABLE_LOCATION MODIFIED'
    query_list = ["PRAGMA", CREATE_TABLE_QUERY['SYS_TABLE_LOCATION'], CREATE_INDEX_QUERY['SYS_TABLE_LOCATION']]
    _execute_query("SYS_TABLE_LOCATION/%s" % table_name, query_list) 

def _create_local_sys(table_name, key, partition, ram_exp=30, disk_exp=34200, partition_range=60):
    _create_sys_table_location(table_name)
    print '    !!! SYS_TABLE_INFO MODIFIED'
    _execute_query("SYS_TABLE_INFO", [SYSTEM_QUERY['INSERT_TABLE_INFO_STRING_LOCAL'] % ( table_name, 'LOCAL', CREATE_TABLE_QUERY[table_name], ram_exp, 0, disk_exp, key, partition_range, partition )])
    print '    !!! SYS_TABLE_PRIV MODIFIED'
    _execute_query("SYS_TABLE_PRIV", [SYSTEM_QUERY['INSERT_TABLE_PRIV_STRING'] % ( table_name, 1, 1, 1, 1, 0, 0 )])

def create_local_sys_all():
    print '!!! MAKE SYS_SSD_INFO BACKEND'
    _create_local_sys("SYS_SSD_INFO", "NODE_ID", "UPDATETIME")

def main():
    create_local_sys_all()   

if __name__ == "__main__":
    main()
