import M6.Common.Default as Default
import socket
import sys
import cPickle

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


def find_test():
	table_name = 'TEST1'
	table_key = 'k10'
	table_part = '20170801000000'
	dst_ip = '192.168.123.203'
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

	head_line = 1
	head_info = 'FILE=/home/iris/IRIS/slave_disk/part00/%s/%s/%s/%s/%s/%s_%s_%s.DAT' % (table_name, table_key, table_part[0:4], table_part[4:6], table_part[6:8], table_name, table_key, table_part)

	query = 'SELECT COUNT(*) FROM %s' % table_name
	param = '%s\n%s\n%s' % (head_line, head_info, query)

	param_size = len(param)
	param = 'EXECUTE_PICKLE %s\n%s' % (param_size, param)
	sock.sendall(param)

	fetch_data = []
	while True:
		msg = s.readline()
		if msg[0] == '-':
			return msg

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

	return msg


if __name__ == "__main__":
	my_ip = Default.NODE_IP
	fail_list = []
	
	ret_message = dsd_test()
	print 'ret_message : ', ret_message
	if ret_message[0] == '-':
		print ret_message

