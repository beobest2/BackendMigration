# coding: utf-8
import md5
import hashlib
import M6
import M6.Common.Default as Default
import M6.Common.Server.TCPThreadServer as TCPThreadServer

from M6.Common.DB import Backend
from M6.Common.FileSystem import IRISFileSystem
from M6.Common.FileSystem.Storage.DBStorage import BackendLocator
from M6.Common.FileSystem.Storage.DiskStorage import PathMaker

from socket import error as SocketError
from M6.Common.Protocol.Socket import Socket as Socket

# backend migration daemon
class BMD(object):
	def __init__(self, sock, temp):
		object.__init__(self)
		## client socket
		self.sock = sock
		self.WELCOME = "+OK Welcome BMD Server\r\n"
		self.buffer_size = 4096
		print "init fin "	
	def run(self):
		print "run start ~"
		self.sock.SendMessage(self.WELCOME)

		while True:
			try:
                
				line = self.sock.Readline().strip()

				cmd_list = line.split(" ", 1)
				cmd = cmd_list[0].upper()
				print "cmd : ", cmd
				if cmd == "QUIT":
					self.sock.SendMessage("+OK BYE\r\n")
					break
				elif cmd == "SEND":
                    # target node 로 백엔드 복사
					ret_message = self.SEND(cmd_list[1])
				elif cmd == "RECV":
                    # 백엔드 받음
					ret_message = self.RECV(cmd_list[1])
				elif cmd == "DELETE":
                    # 백엔드 삭제
					ret_message = self.DELETE(cmd_list[1])
				elif cmd == "TEST":
                    # 백엔드 전송과 메타데이터 변경이 제대로 수행되었는지 테스팅
					ret_message = self.TEST(cmd_list[1])
				elif cmd == "DLDADD":
                    # 마스터 노드의 DLD 정보 추가
					ret_message = self.DLDADD(cmd_list[1])
				elif cmd == "DLDDEL":
                    # 마스터 노드의 DLD 정보 삭제
					ret_message = self.DLDDEL(cmd_list[1])
				elif cmd == "LDLDADD":
                    # 슬레이브 노드의 LDLD 정보 추가
					ret_message = self.LDLDADD(cmd_list[1])
				elif cmd == "LDLDDEL":
                    # 슬레이브 노드의 LDLD 정보 삭제
					ret_message = self.LDLDDEL(cmd_list[1])
				else:
					ret_message = "-ERR Invalid Command (%s)\r\n" % cmd

				self.sock.SendMessage(ret_message)

			except self.sock.SocketDisconnectException:
				break

			except Exception, err:
				self.sock.SendMessage("-ERR %s\r\n" % str(err))
				break

			try:
				self.sock.close()
			except:
				pass
                
	def SEND(self, param):
		print "SEND", param
		ret_message = "+OK SUCCESS\r\n"
		# ex) SEND 192.168.111.221:~/IRIS/data/slave/~~.dat:~/IRIS/data/slave/~~.dat
		# paramList = [dst_ip, src_file_path, dst_file_path]
		paramList = param.strip().split(":")
		dst_ip = paramList[0]
		src_file_path = paramList[1]
		dst_file_path = paramList[2]

		try:
			fd = open(src_file_path, "r")
			data = fd.read()
		except:
			return "-ERR file read error\r\n"
		finally:
			try:
				fd.close()
			except:
				pass

		file_size = len(data)
		hash = md5.new()
		hash.update(data)
		hash_value = hash.hexdigest()
		send_message = "RECV %d:%s\r\n" % (file_size, hash_value)
		self.sock.SendMessage(send_messsage)

		s_index = 0
		retry_count = 0
		max_retry_count = 3
		
		while True:
			while True:
				s_data = data[s_index:s_index+self.buffer_size]
				self.sock.SendMessage(s_data)
				if len(s_data) != self.buffer_size:
					break
				else:
					s_index += self.buffer_size

			recv_result = self.sock.Readline()
			if recv_result[0] != '+':
				if retry_count < max_retry_count:
					retry_count += 1
					self.sock.SendMessage("+OK retry\r\n")
					s_index = 0
					continue
				else:
					data = None
					return "-ERR"
			break
		data = None
		return ret_message


	def RECV(self, param):
		print "RECV" , param
		ret_message = "+OK SUCCESS\r\n"
		# param = file_size:hash_value
		paramList = param.strip().split(":")
		data_length = param[0]
		hex_data = param[1]

		while True:
			try:
				data = self.sock.Read(data_length)
				hash = md5.new()
				hash.update(data)
				hash_value = hash.hexdigest()
			except:
				hex_data = None
			if hex_data == hash_value:
				self.sock.SendMessage("+OK data is ok\r\n")
				break
			else:
				self.sock.SendMessage("-ERR data is not ok\r\n")
				retry_msg = self.sock.Readline()
				if retry_msg[0] == '-':
					return retry_msg

		return ret_message

    def DELETE(param):
        # param : table_name, key, partition, src_ip, dst_ip
        # This function operates in data node
        # backend : src_ip -> dst_ip
        # del existing backendfile 
        ret_message = "+OK DELETE SUCCESS\r\n"

        paramList = param.split(',')
        if len(paramList) != 5:
            ret_message = "-ERR DELETE param error!\r\n"
            return

        table_name = paramList[0].upper()
        key = paramList[1]
        partition = paramList[2]

        try:
            file_path = IRISFileSystem.exists(table_name, key, partition)

            if file_path is None:
                file_path = PathMaker.make_base_path(table_name, key, partition)
                ret_message = "-ERR DELETE %s Backend Not Exists \r\n" % file_path
                return ret_message

            os.remove(file_path)
        except Exception, err:
            ret_message = "-ERR DELETE %s\r\n" % str(err)

        return ret_message

	def TEST(self, param):
		ret_message = "+OK TEST SUCCESS\r\n"

		return ret_message

	def DLDADD(self, param):
        # param : table_name, key, partition, src_ip, dst_ip
        # This function operates in master node
        # backend : src_ip -> dst_ip
        # add location info to dld for new backend

		ret_message = "+OK DLDADD SUCCESS\r\n"

        paramList = param.split(',')
        if len(paramList) != 5:
            ret_message = "-ERR DLDADD param error!\r\n"
            return

        table_name = paramList[0].upper()
        key = paramList[1]
        partition = paramList[2]
        dst_ip = paramList[4]

        SYS_TABLE_LOCATION = "%s/SYS_TABLE_LOCATION/%s.DAT" % (Default.M6_MASTER_DATA_DIR, table_name)
        SYS_NODE_INFO = "%s/SYS_NODE_INFO.DAT" % (Default.M6_MASTER_DATA_DIR)

        find_nodeid_query = "SELECT NODE_ID FROM SYS_NODE_INFO WHERE IP_ADDRESS_1 = '%s'" % dst_ip
    
        try :
            backend = Backend([SYS_NODE_INFO, SYS_TABLE_LOCATION])
            conn = backend.GetConnection()

            c = conn.cursor()

            c.execute(find_nodeid_query)
            node_id = int(c.fetchone()[0])
    
            add_dld_query = "INSERT INTO SYS_TABLE_LOCATION (TABLE_KEY, TABLE_PARTITION, NODE_ID, STATUS)\
                        VALUES ( '%s', '%s', '%s', 'C' );" % (key, partition, node_id)
        
            c.execute(add_dld_query)
        except Exception, err:
            ret_message = "-ERR DLDADD %s\r\n" % str(err)
        finally:
            try: c.close()
            except: pass
            try: conn.close()
            except: pass
        
        return ret_message
 
	def DLDDEL(self, param):
        # param : table_name, key, partition, src_ip, dst_ip
        # This function operates in master node
        # backend : src_ip -> dst_ip
        # del existing location info to dld for deleted backend

		ret_message = "+OK DLDDEL SUCCESS\r\n"

        paramList = param.split(',')
        if len(paramList) != 5:
            ret_message = "-ERR DLDDEL param error!\r\n"
            return

        table_name = paramList[0].upper()
        key = paramList[1]
        partition = paramList[2]
        src_ip = paramList[4]

        SYS_TABLE_LOCATION = "%s/SYS_TABLE_LOCATION/%s.DAT" % (Default.M6_MASTER_DATA_DIR, table_name)
        SYS_NODE_INFO = "%s/SYS_NODE_INFO.DAT" % (Default.M6_MASTER_DATA_DIR)

        find_nodeid_query = "SELECT NODE_ID FROM SYS_NODE_INFO WHERE IP_ADDRESS_1 = '%s'" % src_ip
    
        try :
            backend = Backend([SYS_NODE_INFO, SYS_TABLE_LOCATION])
            conn = backend.GetConnection()

            c = conn.cursor()

            c.execute(find_nodeid_query)
            node_id = int(c.fetchone()[0])
    
            del_dld_query = "DELETE FROM SYS_TABLE_LOCATION WHERE TABLE_KEY = '%s' AND \
                            TABLE_PARTITION = '%s' AND \
                            NODE_ID = '%s';"
        
            c.execute(del_dld_query)
        except Exception, err:
            ret_message = "-ERR DLDDEL %s\r\n" % str(err)
        finally:
            try: c.close()
            except: pass
            try: conn.close()
            except: pass
        
		return ret_message

    def LDLDADD(param):
        # param : table_name, key, partition, src_ip, dst_ip
        # This function operates in data node
        # backend : src_ip -> dst_ip
        # add new location info to ldld for added backend
        ret_message = "+OK LDLDADD SUCCESS\r\n"

        paramList = param.split(',')
        if len(paramList) != 5:
            ret_message = "-ERR LDLDADD param error!\r\n"
            return

        table_name = paramList[0].upper()
        key = paramList[1]
        partition = paramList[2]
        link_name = 'part00'

        try:
            file_path = IRISFileSystem.exists(table_name, key, partition)

            if 'ssd_data' in file_path:
                target = 'SSD'
            elif 'slave_disk' in file_path:
                target = 'HDD'
            else
                target = 'RAM'

            BackendLocator.insert_record(table_name, key, partition, target, link_name)
        except Exception, err:
            ret_message = "-ERR LDLDADD %s\r\n" % str(err)

        return ret_message


	def LDLDDEL(self, param):
        # param : table_name, key, partition, src_ip, dst_ip
        # This function operates in data node
        # backend : src_ip -> dst_ip
        # del existing location info to ldld for deleted backend
        ret_message = "+OK LDLDDEL SUCCESS\r\n"

        paramList = param.split(',')
        if len(paramList) != 5:
            ret_message = "-ERR LDLDDEL param error!\r\n"
            return

        table_name = paramList[0].upper()
        key = paramList[1]
        partition = paramList[2]

        try:
            #file_path = IRISFileSystem.exists(table_name, key, partition)

            #if file_path is None:
            #    file_path = PathMaker.make_base_path(table_name, key, partition)
            #    ret_message = "-ERR %s Backend Not Exists \r\n" % file_path
            #    return ret_message

            BackendLocator.delete_record(table_name, key, partition)
        except Exception, err:
            ret_message = "-ERR LDLDDEL %s\r\n" % str(err)

        return ret_message


if __name__ == '__main__':
	print "Hello"
	port = 9999

	server = TCPThreadServer.Server(port, BMD, None)
	server.start()
	
