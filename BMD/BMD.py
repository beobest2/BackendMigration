# coding: utf-8

from M6.Common.FileSystem import IRISFileSystem
from M6.Common.FileSystem.Storage.DBStorage import BackendLocator
from M6.Common.FileSystem.Storage.DiskStorage import DiskStorage
from M6.Common.FileSystem.Storage.DiskStorage import PathMaker
from M6.Common.Protocol.ClientProc import Client 
from M6.Common.Protocol.ClientProc import __SendCMD__

import sys
import os
import os.path
import md5
import hashlib
import socket
import cPickle
import M6
import M6.Common.Server.TCPThreadServer as TCPThreadServer
from M6.Common.DB import Backend
import M6.Common.Default as Default

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
		self.port = 9999
		#print "init "

	def run(self):
		self.sock.SendMessage(self.WELCOME)
		while True:
			try:
				line = self.sock.Readline().strip()
				cmd_list = line.split(" ", 1)
				cmd = cmd_list[0].upper()
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
				elif cmd == "DSDTEST":
                    # 백엔드 전송과 메타데이터 변경이 제대로 수행되었는지 테스팅
					ret_message = self.DSDTEST(cmd_list[1])
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
		#print "SEND", param
		ret_message = "+OK SEND SUCCESS\r\n"
		# ex) SEND table_name,key,partition,src_ip,dst_ip
		# paramList = [table_name,key,partition, src_ip, dst_ip]
		
		paramList = param.strip().split(',')
		if len(paramList) != 5:
			ret_message = "-ERR SEND param error!\r\n"
			return
			
		table_name = paramList[0].upper()
		key = paramList[1]
		partition = paramList[2]
		src_ip = paramList[3]
		dst_ip = paramList[4]
		
		#print "table_name : ", table_name
		#print "key : ", key
		#print "partition : ", partition
		
		#src_file_path = DiskStorage.exists(table_name, key, partition)
		src_file_path = IRISFileSystem.exists(table_name, key, partition)
		#print "src_file_path: ", src_file_path
		if src_file_path == None:
			return "-ERR there is no backend at src\r\n"
		dst_file_path = src_file_path
		
		#print "file_path : ", dst_file_path 

		try:
			fd = open(src_file_path, "rb")
			data = fd.read()
			#print "read data : ", data
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

		tkp = table_name + ',' + key + ',' + partition
		send_message = "RECV %d:%s:%s:%s\r\n" % (file_size, hash_value, dst_file_path, tkp)
		
		s = Socket()
		s.Connect(dst_ip, self.port)
		s.ReadMessage()

		s.SendMessage(send_message)

		s_index = 0
		retry_count = 0
		max_retry_count = 3
		
		while True:
			while True:
				s_data = data[s_index:s_index+self.buffer_size]
				s.SendMessage(s_data)
				if len(s_data) != self.buffer_size:
					break
				else:
					s_index += self.buffer_size

			recv_result = s.Readline()
			if recv_result[0] != '+':
				if retry_count < max_retry_count:
					retry_count += 1
					s.SendMessage("+OK retry\r\n")
					s_index = 0
					continue
				else:
					data = None
					return "-ERR"
			break
		data = None
		recv_final_msg = s.ReadMessage()
		if recv_final_msg[0] == False:
			ret_message = "-ERR " + recv_final_msg[1]

		s.close()
           
		return ret_message


	def RECV(self, param):
		#print "RECV" , param
		ret_message = "+OK RECV SUCCESS\r\n"
		hash_value = ''
		# param = file_size:hash_value:file_path:tkp
		paramList = param.strip().split(":")
		#print paramList

		data_length = paramList[0]
		hex_data = paramList[1]
		dst_file_path = paramList[2]
		tkp = paramList[3]

		tkpList = tkp.strip().split(",")
		table_name = tkpList[0].upper()
		key = tkpList[1]
		partition = tkpList[2]

		#print "data length : ", data_length
		#print "hex data : ", hex_data
		#print "dst file path : ", dst_file_path
		#print "table_name : ", table_name
		#print "key : ", key
		#print "partition : ", partition
			
		while True:
			try:
				data = self.sock.Read(int(data_length))
				#print "data : ", data
				hash = md5.new()
				hash.update(data)
				hash_value = hash.hexdigest()
			except Exception  ,e:
				print "err ", e.message
				hash_value = None

			#print "hex_data : ", hex_data
			#print "hash_value : ", hash_value

			if hex_data == hash_value:
				self.sock.SendMessage("+OK data is ok\r\n")
				break
			else:
				self.sock.SendMessage("-ERR data is not ok\r\n")
				retry_msg = self.sock.Readline()
				if retry_msg[0] == '-':
					return retry_msg
		try:
			path = os.path.dirname(dst_file_path)
			if os.path.exists(path) == False:
				#dst_file_path 경로 생성
				os.makedirs(path)
			
			fd = open(dst_file_path, "wb")
			fd.write(data)
		except Exception, e:
			#print "-err file write error !" + str(e)
			return "-ERR file write error" + e.message + "\r\n"
		finally:
			try:
				fd.close()
			except:
				pass

		return ret_message

	def DELETE(self, param):
		# param : table_name, key, partition, src_ip, dst_ip
		## This function operates in data node
		# backend : src_ip -> dst_ip
		# del existing backendfile 
		ret_message = "+OK DELETE SUCCESS\r\n"

		paramList = param.strip().split(',')
		if len(paramList) != 5:
			ret_message = "-ERR DELETE param error!\r\n"
			return ret_message

		table_name = paramList[0].upper()
		key = paramList[1]
		partition = paramList[2]

		try:
			file_path = DiskStorage.exists(table_name, key, partition)
			#print table_name
			#print key
			#print partition
			#print file_path

			if file_path is None:
				file_path = PathMaker.make_base_path(table_name, key, partition)
				ret_message = "-ERR DELETE %s Backend Not Exists \r\n" % file_path
				return ret_message

			DiskStorage.remove_backend(table_name, key, partition)
		except Exception, err:
			ret_message = "-ERR DELETE %s\r\n" % str(err)

		return ret_message

	def DSDTEST(self, param):
		paramList = param.strip().split(',')
		if len(paramList) != 5:
			msg = "-ERR DSDTEST param error!\r\n"
			return msg

		table_name = paramList[0].upper()
		table_key = paramList[1]
		table_partition = paramList[2]
		src_ip = paramList[3]
		dst_ip = paramList[4]
		dsd_port =  Default.PORT['DSD']
		
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((dst_ip, dsd_port))
		s = sock.makefile("rb")

		#connect OK message
		msg = s.readline()
		if msg[0] == '-':
			return msg

		#field separator
		sock.sendall('SET_FIELD_SEP %s\r\n' % Default.FIELD_SEPARATOR)
		msg = s.readline()
		if msg[0] == '-':
			return msg

		sock.sendall('SET_RECORD_SEP %s\r\n' % Default.RECORD_SEPARATOR)
		msg = s.readline()
		if msg[0] == '-':
			return msg

		file_path = IRISFileSystem.exists(table_name, table_key, table_partition)

		if file_path == None:
			msg  = "-ERR No Backend %s, %s, %s\r\n" % (table_name, table_key, table_partition)
			return msg

		head_line = 1
		head_info = 'FILE=%s' % (file_path)

		query = 'SELECT COUNT(*) FROM %s' % table_name
		param = '%s\n%s\n%s' % (head_line, head_info, query)

		param_size = len(param)
		param = 'EXECUTE_PICKLE %s\n%s' % (param_size, param)
		sock.sendall(param)

		fetch_data = []
		while True:
			msg = s.readline()

			if msg != None and len(msg.strip().split(" ", 1)) == 2:
				_type, param = msg.strip().split(" ", 1)

			if msg == None:
				break
			elif _type.upper() in ("+OK", "-ERR"):
				result = msg
				break

			data_length = int(param)
			if data_length == 0:
				break

			data = s.read(data_length)
			fetch_data += cPickle.loads(data)

			sock.sendall('CONT_PICKLE\r\n')

		s.close()
		sock.close()

		if msg[0] == '+':
			msg = '+OK DSDTEST SUCCESS\r\n'

		return msg

	def DLDADD(self, param):
        # param : table_name, key, partition, src_ip, dst_ip
        # This function operates in master node
        # backend : src_ip -> dst_ip
        # add location info to dld for new backend

		ret_message = "+OK DLDADD SUCCESS\r\n"
		paramList = param.strip().split(',')
		if len(paramList) != 5:
			ret_message = "-ERR DLDADD param error!\r\n"
			return ret_message

		table_name = paramList[0].upper()
		key = paramList[1]
		partition = paramList[2]
		dst_ip = paramList[4]

		SYS_TABLE_LOCATION = "%s/SYS_TABLE_LOCATION/%s.DAT" % (Default.M6_MASTER_DATA_DIR, table_name)
		SYS_NODE_INFO = "%s/SYS_NODE_INFO.DAT" % (Default.M6_MASTER_DATA_DIR)

		find_nodeid_query = "SELECT NODE_ID FROM SYS_NODE_INFO WHERE IP_ADDRESS_1 = '%s';" % dst_ip

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
		src_ip = paramList[3]

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
							NODE_ID = '%s';" % (key, partition, node_id)
       
			#print del_dld_query 
			c.execute(del_dld_query)
		except Exception, err:
			ret_message = "-ERR DLDDEL %s\r\n" % str(err)
		finally:
			try: c.close()
			except: pass
			try: conn.close()
			except: pass
        
		return ret_message

	def LDLDADD(self, param):
        # param : table_name, key, partition, src_ip, dst_ip, mount_path
        # This function operates in data node
        # backend : src_ip -> dst_ip
        # add new location info to ldld for added backend
		ret_message = "+OK LDLDADD SUCCESS\r\n"

		paramList = param.strip().split(',')
		if len(paramList) != 6:
			ret_message = "-ERR LDLDADD param error!\r\n"
			return ret_message

		table_name = paramList[0].upper()
		key = paramList[1]
		partition = paramList[2]
		mount_path = paramList[5]

		if mount_path == 'HDD' or mount_path == 'SSD':
			link_name = 'part00'
		else:
			link_name = None

		try:
			BackendLocator.insert_record(table_name, key, partition, mount_path, link_name)
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
			file_path = IRISFileSystem.exists(table_name, key, partition)

			if file_path is None:
				file_path = PathMaker.make_base_path(table_name, key, partition)
				ret_message = "-ERR %s Backend Not Exists \r\n" % file_path
				return ret_message

			BackendLocator.delete_record(table_name, key, partition)
		except Exception, err:
			ret_message = "-ERR LDLDDEL %s\r\n" % str(err)

		return ret_message

if __name__ == '__main__':
	print "BMD Server Start "
	port = 9999

	server = TCPThreadServer.Server(port, BMD, None)
	server.start()
	
