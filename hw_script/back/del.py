import sys
import os
import md5
import hashlib
import M6
import M6.Common.Default as Default
import M6.Common.Server.TCPThreadServer as TCPThreadServer

from M6.Common.FileSystem import IRISFileSystem
from M6.Common.FileSystem.Storage.DBStorage import BackendLocator
from M6.Common.FileSystem.Storage.DiskStorage import DiskStorage
from M6.Common.FileSystem.Storage.DiskStorage import PathMaker
from M6.Common.DB import Backend
from socket import error as SocketError
from M6.Common.Protocol.Socket import Socket as Socket

def DEL(param):
    # param : table_name, key, partition, src_ip, dst_ip, mount_path
    # This function operates in data node
    # backend : src_ip -> dst_ip
    # del existing backendfile 
    ret_message = "+OK SUCCESS\r\n"

    paramList = param.split(',')
    if len(paramList) != 5:
        ret_message = "-ERR param error!\r\n"
        print ret_message
        return

    table_name = paramList[0].upper()
    key = paramList[1]
    partition = paramList[2]

    try:
        file_path = IRISFileSystem.exists(table_name, key, partition)

        if file_path is None:
            file_path = PathMaker.make_base_path(table_name, key, partition)
            ret_message = "-ERR %s Backend Not Exists \r\n" % file_path
            return ret_message

        os.remove(file_path)
    except Exception, err:
        ret_message = "-ERR %s\r\n" % str(err)

    return ret_message


if __name__ == '__main__':
    table_name = 'test'
    key = 'k5'
    partition = '20170806040000'
    src_ip = '192.168.123.123'
    dst_ip = '192.168.123.201'

    param = table_name + ',' + key + ',' + partition + ',' + src_ip + ',' + dst_ip
    DEL(param)
