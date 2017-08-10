import sys
import md5
import hashlib
import M6
import M6.Common.Default as Default
import M6.Common.Server.TCPThreadServer as TCPThreadServer

from M6.Common.FileSystem import IRISFileSystem
from M6.Common.FileSystem.Storage.DBStorage import BackendLocator
from M6.Common.FileSystem.Storage.DiskStorage import PathMaker
from M6.Common.DB import Backend
from socket import error as SocketError
from M6.Common.Protocol.Socket import Socket as Socket

def LDLDDEL(param):
    # param : table_name, key, partition, src_ip, dst_ip
    # This function operates in data node
    # backend : src_ip -> dst_ip
    # del existing location info to ldld for deleted backend
    ret_message = "+OK SUCCESS\r\n"

    paramList = param.split(',')
    if len(paramList) != 5:
        ret_message = "-ERR param error!\r\n"
        print ret_message
        return

    table_name = paramList[0].upper()
    key = paramList[1]
    partition = paramList[2]
    src_ip = paramList[4]

    try:
        file_path = IRISFileSystem.exists(table_name, key, partition)

        if file_path is None:
            file_path = PathMaker.make_base_path(table_name, key, partition)
            ret_message = "-ERR %s Backend Not Exists \r\n" % file_path
            print ret_message
            return ret_message
        BackendLocator.delete_record(table_name, key, partition)
    except Exception, err:
        ret_message = "-ERR %s\r\n" % str(err)
        print ret_message

    return ret_message


if __name__ == '__main__':
    table_name = 'test'
    key = 'k12'
    partition = '20170806020000'
    src_ip = '192.168.123.123'
    dst_ip = '192.168.123.201'

    param = table_name + ',' + key + ',' + partition + ',' + src_ip + ',' + dst_ip
    LDLDDEL(param)
