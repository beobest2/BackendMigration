import os
import sqlite3
import csv

PATH = '/home/iris/IRIS/data/monitor_data_test'

def walk_directory(path):
    if not os.path.exists(path):
        print '%s path is not exists !!' % path
        return

    for (path, dir, files) in os.walk(path):
        if not dir:
            for file in files:
                print 'target path      : ', path
                print 'target backend      : ', file
                file_path = os.path.join(path,file)
                # convert data using csv
                make_new_data(file, file_path)
                # delete existing backend file & create new backend file (create schema)
                make_new_backend(file_path)
                # import coverted data
                import_data(file, file_path)
                # delete backup csv
                delte_backup_data(file)
                

def make_new_data(file, file_path):
    conn = sqlite3.connect(file_path)
    cur = conn.cursor()
    sql = "SELECT * FROM TABLE_INFO"
    cur.execute(sql)
    rows = cur.fetchall()

    print '=============================================================================================================='
    print 'make new table_info data : ./%s_table_info_backup.csv'%os.path.splitext(file)[0]

    with open('./%s_table_info_backup.csv'%os.path.splitext(file)[0], 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for row in rows:
			row_list = list(row)
			row_list.insert(7, 0)
			row_list.insert(8, 0)
			writer.writerow(row_list)

    sql = "SELECT * FROM SYS_INFO"
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()

    print 'make new sys_info data : ./%s_sys_info_backup.csv'%os.path.splitext(file)[0]

    with open('./%s_sys_info_backup.csv'%os.path.splitext(file)[0], 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for row in rows:
			row_list = list(row)
			row_list.insert(28,0)
			row_list.insert(29,0)
			writer.writerow(row_list)

def make_new_backend(file_path):
    delete_cmd = 'rm %s' % file_path
    make_backend_cmd = 'touch %s' % file_path
    os.system(delete_cmd)
    os.system(make_backend_cmd)

    print 'make_new_backend on ', file_path

    conn = sqlite3.connect(file_path)
    cur = conn.cursor()
    table_info_sql = '''
    CREATE TABLE TABLE_INFO(
        UPDATE_TIME TEXT,
        NODE_ID NUMBER,
        TABLE_NAME TEXT,
        TABLE_SIZE NUMBER,
        NUM_OF_FILE NUMBER,
        TABLE_SIZE_RAM NUMBER,
        NUM_OF_FILE_RAM NUMBER,
        TABLE_SIZE_SSD NUMBER,
        NUM_OF_FILE_SSD NUMBER
    );
    '''
    cur.execute(table_info_sql)
   
    sys_info_sql  = '''
    CREATE TABLE SYS_INFO (
         UPDATE_TIME TEXT,
         NODE_ID NUMBER,
         NODE_IP TEXT,
         SYS_STATUS TEXT,
         ADM_STATUS TEXT,
         HOST_NAME TEXT,
         OS_NAME TEXT,
         OS_VERSION TEXT,
         OS_TYPE TEXT,
         NET_NAME TEXT,
         NET_TYPE TEXT,
         NET_MAC TEXT,
         NET_IN_PACKET NUMBER,
         NET_OUT_PACKET NUMBER,
         NET_IN_BYTE NUMBER,
         NET_OUT_BYTE NUMBER,
         CPU_CLOCK NUMBER,
         CPU_CORE NUMBER,
         CPU_USAGE NUMBER,
         CPU_L_AVG NUMBER,
         CPU_IOWAIT NUMBER,
         RAM_TOTAL NUMBER,
         RAM_USAGE_FILE NUMBER,
         RAM_USAGE_PROCESS NUMBER,
         RAM_SWAP_TOTAL NUMBER,
         RAM_SWAP_USAGE NUMBER,
         HDD_TOTAL NUMBER,
         HDD_USAGE NUMBER,
		 SSD_TOTAL NUMBER,
		 SSD_USAGE NUMBER
     );
    '''
    cur.execute(sys_info_sql)

    conn.close()

def import_data(file, file_path):
    conn = sqlite3.connect(file_path)
    cur = conn.cursor()

    print 'import new table_info data : ./%s_table_info_backup.csv'%os.path.splitext(file)[0]
    print 'import to  : ', file_path

    with open('./%s_table_info_backup.csv'%os.path.splitext(file)[0], 'rb') as csvfile:
        reader  = csv.reader(csvfile, delimiter=',')
        for field in reader:
            cur.execute("INSERT INTO TABLE_INFO VALUES (?,?,?,?,?,?,?,?,?);", field)
    conn.commit()
   
    print 'import new sys_info data : ./%s_sys_info_backup.csv'%os.path.splitext(file)[0]
    print 'import to  : ', file_path

    with open('./%s_sys_info_backup.csv'%os.path.splitext(file)[0], 'rb') as csvfile:
        reader  = csv.reader(csvfile, delimiter=',')
        for field in reader:
            cur.execute("INSERT INTO SYS_INFO VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", field)
    conn.commit()

    conn.close()

def delte_backup_data(file):

    print 'rm ./%s_table_info_backup.csv ./%s_sys_info_backup.csv' % (os.path.splitext(file)[0], os.path.splitext(file)[0])
    print '=============================================================================================================='
    print

    delete_backup_backend_cmd = 'rm ./%s_table_info_backup.csv ./%s_sys_info_backup.csv' % (os.path.splitext(file)[0], os.path.splitext(file)[0])
    os.system(delete_backup_backend_cmd)

def make_backup():
    os.system('cp -rf ~/IRIS/data/monitor_data ~/IRIS/data/monitor_data_test')

def main():
    make_backup()
    walk_directory(PATH)

if __name__ == '__main__':
    main()
