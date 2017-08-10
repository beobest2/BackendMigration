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

def LDLDADD(param):
    # param : table_name, key, partition, src_ip, dst_ip
    # This function operates in data node
    # backend : src_ip -> dst_ip
    # add new location info to ldld for added backend
    ret_message = "+OK SUCCESS\r\n"

    paramList = param.split(',')
    if len(paramList) != 5:
        ret_message = "-ERR param error!\r\n"
        print ret_message
        return

    table_name = paramList[0].upper()
    key = paramList[1]
    partition = paramList[2]
    #fixme : paramlist tablename, key, partition, src_ip, dst_ip, link

    try:
        link_name = 'part00'
        BackendLocator.insert_record(table_name, key, partition, target, link_name)
    except Exception, err:
        ret_message = "-ERR %s\r\n" % str(err)
        print ret_message

    return ret_message


if __name__ == '__main__':
    table_name = 'test'
    key = 'tk4'
    partition = '20170806020000'
    src_ip = '192.168.123.123'
    dst_ip = '192.168.123.201'

    param = table_name + ',' + key + ',' + partition + ',' + src_ip + ',' + dst_ip
    LDLDADD(param)
