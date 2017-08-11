import sys
import datetime
import M6.Common.Default as Default
import API.M6 as M6
from M6.Common.DB import Backend
from M6.Common.DB import BackendReader

backend_list = dict()
backend_number = dict()
    
def get_local_backend_count(table_name, partition_start, partition_end):
    sys_table_location = '%s/%s' % (Default.M6_MASTER_DATA_DIR, 'SYS_TABLE_LOCATION')
    table_path = "%s/%s.DAT" % (sys_table_location, table_name.upper())

    query = 'SELECT COUNT(*) FROM SYS_TABLE_LOCATION WHERE SYS_TABLE_LOCATION.TABLE_PARTITION >= %s AND SYS_TABLE_LOCATION.TABLE_PARTITION <= %s' % (partition_start, partition_end)

    with BackendReader(table_path) as reader:
        reader.cursor.execute(query)
        total_backend_count = reader.cursor.fetchone()[0]

        if total_backend_count == 0:
            raise Exception("Table :%s , Partition : %s ~ %s Not Exists!" % (table_path, partition_start, partition_end))
        else:
            return total_backend_count

def get_location_hint(partition_start):
    p_datetime = datetime.datetime.strptime(partition_start, '%Y%m%d%H%M%S')
    p_endtime = p_datetime + datetime.timedelta(days=1)
    partition_end = datetime.datetime.strftime(p_endtime, '%Y%m%d%H%M%S')
    return partition_end
 
def make_data_migration_info(total_backend_count, table_name, partition_start, partition_end, total_data_node, add_ip_list):
    max_backend_per_node = total_backend_count/total_data_node

    result = get_backend_data(table_name, partition_start, partition_end)

    for target_ip_address in add_ip_list:
        backend_number[target_ip_address] = 0
        backend_list[target_ip_address] = []

    mfile = open('./migration_info.dat', 'w')

    for row in result:
        table_key = row[0]
        table_partition = row[1]
        src_ip = row[2]

        table_info = table_name + ',' + table_key + ',' + table_partition 
        migration_info = table_name + ',' + table_key + ',' + table_partition + ',' + src_ip 
       
        for target_ip_address in add_ip_list:
            if backend_number[target_ip_address] >= max_backend_per_node:
                continue
            else:
                if table_info not in backend_list[target_ip_address]:
                    backend_list[target_ip_address].append(table_info)
                    backend_number[target_ip_address] += 1
                    mfile.write(migration_info + ',' + target_ip_address + '\n')
                    break
                else:
                    continue
   
    print backend_list
    print backend_number 
    mfile.close()

def get_backend_data(table_name, partition_start, partition_end):
    SYS_NODE_INFO = "%s/SYS_NODE_INFO.DAT" % Default.M6_MASTER_DATA_DIR
    SYS_TABLE_LOCATION = "%s/SYS_TABLE_LOCATION/%s.DAT" % (Default.M6_MASTER_DATA_DIR, table_name.upper())

    backend = Backend([SYS_NODE_INFO, SYS_TABLE_LOCATION])
    conn = backend.GetConnection()
    c = conn.cursor()

    query = 'SELECT TABLE_KEY, TABLE_PARTITION, IP_ADDRESS_1 \
    FROM SYS_TABLE_LOCATION \
    LEFT JOIN SYS_NODE_INFO \
    ON SYS_TABLE_LOCATION.NODE_ID = SYS_NODE_INFO.NODE_ID \
    WHERE \
    SYS_TABLE_LOCATION.TABLE_PARTITION >= %s \
    AND SYS_TABLE_LOCATION.TABLE_PARTITION <= %s' % (partition_start, partition_end)

    c.execute(query)

    result = []
    for row in c:
        result.append(row)

    c.close()
    conn.close()

    return result

if __name__ == '__main__':
    total_data_node = 0
    add_ip_list = []

    f = open("./config", "r")
    total_data_node = int(f.readline())
    while True:
        line = f.readline()
        if not line:
            break
        add_ip_list.append(line.strip())

    f.close()

    print "total data node count : ", total_data_node
    print "add ip list : ", add_ip_list

    partition_start = raw_input("Partiton Date : ")

    if (len(partition_start) != 14):
        print 'Partition Date Format Wrong!'
        sys.exit(1)

    table_name = raw_input("Table Name : ")

    if (len(partition_start) == 0 or len(table_name) == 0):
        print 'Input Partition Date and Table Name !'
        sys.exit(1)
    
    partition_end = get_location_hint(partition_start)
    total_backend_count = get_local_backend_count(table_name, partition_start, partition_end)
    make_data_migration_info(total_backend_count, table_name, partition_start, partition_end, total_data_node, add_ip_list)

