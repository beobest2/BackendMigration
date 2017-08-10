import sys
import md5
import hashlib
import M6
import M6.Common.Default as Default
import M6.Common.Server.TCPThreadServer as TCPThreadServer

from M6.Common.DB import Backend
from socket import error as SocketError
from M6.Common.Protocol.Socket import Socket as Socket

def DLDDEL(param):
    # param : table_name, key, partition, src_ip, dst_ip
    # This function operates in master node
    # backend : src_ip -> dst_ip
    # del existing location info to dld for deleted backend
    #
    ret_message = "+OK SUCCESS\r\n"

    paramList = param.split(',')
    if len(paramList) != 5:
        ret_message = "-ERR param error!\r\n"
        print ret_message
        return

    table_name = paramList[0]
    key = paramList[1]
    partition = paramList[2]
    src_ip = paramList[4]

    SYS_TABLE_LOCATION = "%s/SYS_TABLE_LOCATION/%s.DAT" % (Default.M6_MASTER_DATA_DIR, table_name.upper())
    SYS_NODE_INFO = "%s/SYS_NODE_INFO.DAT" % (Default.M6_MASTER_DATA_DIR)

    find_nodeid_query = "SELECT NODE_ID FROM SYS_NODE_INFO WHERE IP_ADDRESS_1 = '%s'" % src_ip

    try :
        backend = Backend([SYS_NODE_INFO, SYS_TABLE_LOCATION])
        conn = backend.GetConnection()

        c = conn.cursor()

        c.execute(find_nodeid_query)
        node_id = int(c.fetchone()[0])

        del_dld_query = "DELETE FROM SYS_TABLE_INFO WHERE TABLE_KEY = '%s' AND \
                        TABLE_PARTITION = '%s' AND \
                        NODE_ID = '%s';" % (key, partition, node_id)

    
        c.execute(del_dld_query)
    except Exception, err:
        ret_message = "-ERR %s\r\n" % str(err)
        print ret_message
    finally:
        try: c.close()
        except: pass
        try: conn.close()
        except: pass
            
    return ret_message


if __name__ == '__main__':
    table_name = 'test'
    key = 'k200'
    partition = '20120616000000'
    src_ip = '192.168.123.123'
    dst_ip = '192.168.123.201'

    param = table_name + ',' + key + ',' + partition + ',' + src_ip + ',' + dst_ip
    DLDDEL(param)
