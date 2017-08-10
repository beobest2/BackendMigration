import M6.Common.Default as Default

from M6.Common.FileSystem import IRISFileSystem
from M6.Common.Protocol.ClientProc import Client as ClientProc
from M6.Common.Protocol.ClientProc import __SendCMD__


class Client(ClientProc):
	def __init__(self, ip = '0.0.0.0' , port  = 9999) :
		ClientProc.__init__(self, ip, port)

	def Connect(self):
		ClientProc.Connect(self)

	def Close(self):
		ClientProc.Close(self)

	def SendCMD(self, msg, multiLine = False):
		return ClientProc.SendCMD(self, msg, multiLine)


def backend_send(paramList):
	table_name = paramList[0].upper()
	table_key = paramList[1]
	table_partition = paramList[2]
	src_ip = paramList[3]
	dst_ip = paramList[4]

	cmd = 'SEND %s,%s,%s,%s,%s\r\n' % (table_name, table_key, table_partition, src_ip, dst_ip)

	c = Client(src_ip, 9999)
	c.Connect()
	ret_message = c.SendCMD(cmd)
	c.Close()

	return ret_message

def add_ldld(paramList):
	table_name = paramList[0].upper()
	table_key = paramList[1]
	table_partition = paramList[2]
	src_ip = paramList[3]
	dst_ip = paramList[4]

	file_path = IRISFileSystem.exists(table_name, table_key, table_partition)

	if file_path == None:
		ret_message = ["-ERR No Backend %s, %s, %s\r\n" % (table_name, table_key, table_partition)]

	if 'ssd' in file_path:
		target = 'SSD'
	elif 'disk' in file_path:
		target = 'HDD'
	else:
		target = 'RAM'

	cmd = 'LDLDADD %s,%s,%s,%s,%s,%s\r\n' % (table_name, table_key, table_partition, src_ip, dst_ip, target)

	c = Client(dst_ip, 9999)
	c.Connect()
	ret_message = c.SendCMD(cmd)
	c.Close()

	return ret_message

def del_ldld(paramList):
	table_name = paramList[0].upper()
	table_key = paramList[1]
	table_partition = paramList[2]
	src_ip = paramList[3]
	dst_ip = paramList[4]

	cmd = 'LDLDDEL %s,%s,%s,%s,%s\r\n' % (table_name, table_key, table_partition, src_ip, dst_ip)

	c = Client(src_ip, 9999)
	c.Connect()
	ret_message = c.SendCMD(cmd)
	c.Close()

	return ret_message

def add_dld(paramList):
	table_name = paramList[0].upper()
	table_key = paramList[1]
	table_partition = paramList[2]
	src_ip = paramList[3]
	dst_ip = paramList[4]
	master_ip = Default.M6_MASTER_IP_ADDRESS

	cmd = 'DLDADD %s,%s,%s,%s,%s\r\n' % (table_name, table_key, table_partition, src_ip, dst_ip)

	c = Client(master_ip, 9999)
	c.Connect()
	ret_message = c.SendCMD(cmd)
	c.Close()

	return ret_message

def del_dld(paramList):
	table_name = paramList[0].upper()
	table_key = paramList[1]
	table_partition = paramList[2]
	src_ip = paramList[3]
	dst_ip = paramList[4]
	master_ip = Default.M6_MASTER_IP_ADDRESS

	cmd = 'DLDDEL %s,%s,%s,%s,%s\r\n' % (table_name, table_key, table_partition, src_ip, dst_ip)

	c = Client(master_ip, 9999)
	c.Connect()
	ret_message = c.SendCMD(cmd)
	c.Close()

	return ret_message

def del_backend(paramList):
	table_name = paramList[0].upper()
	table_key = paramList[1]
	table_partition = paramList[2]
	src_ip = paramList[3]
	dst_ip = paramList[4]

	cmd = 'DELETE %s,%s,%s,%s,%s\r\n' % (table_name, table_key, table_partition, src_ip, dst_ip)

	c = Client(src_ip, 9999)
	c.Connect()
	ret_message = c.SendCMD(cmd)
	c.Close()

	return ret_message


if __name__ == "__main__":
	my_ip = Default.NODE_IP
	fail_list = []
	with open('./migration_info.dat', 'r') as f:
		for line in f:
			param = line[:-1]
			paramList = param.split(',')
			src_ip = paramList[3]
			dst_ip = paramList[4]

			if my_ip == src_ip:
				ret_message = backend_send(paramList)
				print ret_message
				if ret_message[0][0] == '-':
					fail_list.append(paramList)
					continue
			
				ret_message = add_ldld(paramList)
				print ret_message
				if ret_message[0][0] == '-':
					fail_list.append(paramList)
					continue
				
				ret_message = add_dld(paramList)
				print ret_message
				if ret_message[0][0] == '-':
					fail_list.append(paramList)
					continue

				ret_message = del_dld(paramList)
				print ret_message
				if ret_message[0][0] == '-':
					fail_list.append(paramList)
					continue

				ret_message = del_ldld(paramList)
				print ret_message
				if ret_message[0][0] == '-':
					fail_list.append(paramList)
					continue

				ret_message = del_backend(paramList)
				print ret_message
				if ret_message[0][0] == '-':
					fail_list.append(paramList)
					continue

