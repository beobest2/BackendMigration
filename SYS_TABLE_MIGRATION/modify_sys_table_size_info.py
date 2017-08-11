import os
import sqlite3
import csv

RAM_PATH = '/home/iris/IRIS/data/slave/SYS_TABLE_SIZE_INFO_TEST'
DISK_PATH = '/home/iris/IRIS/data/slave_disk/part00/SYS_TABLE_SIZE_INFO_TEST'

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
    sql = "SELECT * FROM SYS_TABLE_SIZE_INFO"
    cur.execute(sql)
    rows = cur.fetchall()

    print '=============================================================================================================='
    print 'make new table_info data : ./%s_backup.csv'%os.path.splitext(file)[0]

    with open('./%s_backup.csv'%os.path.splitext(file)[0], 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for row in rows:
            row_list = list(row)
            row_list.insert(6, 0)
            row_list.insert(7, 0)

            writer.writerow(row_list)

    conn.close()

def make_new_backend(file_path):
    delete_cmd = 'rm %s' % file_path
    make_backend_cmd = 'touch %s' % file_path
    os.system(delete_cmd)
    os.system(make_backend_cmd)

    print 'make_new_backend on ', file_path

    conn = sqlite3.connect(file_path)
    cur = conn.cursor()
    sql = """
        CREATE TABLE SYS_TABLE_SIZE_INFO (
            NODE_ID NUMBER,
            TABLE_NAME TEXT,
            SIZE NUMBER,
            FNUM NUMBER,
            SIZE_RAM NUMBER,
            FNUM_RAM NUMBER,
            SIZE_SSD NUMBER,
            FNUM_SSD NUMBER,
            UPDATETIME NUMBER
            );
        """
    cur.execute(sql)
    conn.close()

def import_data(file, file_path):
    conn = sqlite3.connect(file_path)
    cur = conn.cursor()

    print 'import new table_info data : ./%s_backup.csv'%os.path.splitext(file)[0]
    print 'import to  : ', file_path

    with open('./%s_backup.csv'%os.path.splitext(file)[0], 'rb') as csvfile:
        reader  = csv.reader(csvfile, delimiter=',')
        for field in reader:
            cur.execute("INSERT INTO SYS_TABLE_SIZE_INFO VALUES (?,?,?,?,?,?,?,?,?);", field)

    conn.commit()
    conn.close()

def delte_backup_data(file):
   
    print 'rm ./%s_backup.csv' % (os.path.splitext(file)[0])
    print '=============================================================================================================='
    print

    delete_backup_backend_cmd = 'rm ./%s_backup.csv' % os.path.splitext(file)[0]
    os.system(delete_backup_backend_cmd)

def make_backup():
    if os.path.exists(RAM_PATH):
        os.system('cp -rf ~/IRIS/data/slave/SYS_TABLE_SIZE_INFO ~/IRIS/data/slave/SYS_TABLE_SIZE_INFO_TEST')
    else:
        print '%s path is not exists !!' % RAM_PATH

    os.system('cp -rf ~/IRIS/data/slave_disk/part00/SYS_TABLE_SIZE_INFO ~/IRIS/data/slave_disk/part00/SYS_TABLE_SIZE_INFO_TEST')

def main():
    make_backup()
    walk_directory(RAM_PATH)
    walk_directory(DISK_PATH)

if __name__ == '__main__':
    main()
