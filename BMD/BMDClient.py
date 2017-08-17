import socket
import sys
import cPickle
import datetime
import glob

import M6.Common.Default as Default

from M6.Common.FileSystem import IRISFileSystem
from M6.Common.FileSystem.Storage.DiskStorage import DiskStorage
from M6.Common.Protocol.ClientProc import Client as ClientProc
from M6.Common.Protocol.ClientProc import __SendCMD__
from M6.Common.Protocol.DLDClient import Client as DLDClient

class Client(ClientProc):
	def __init__(self, ip = '0.0.0.0' , port  = 9999) :
		ClientProc.__init__(self, ip, port)

	def Connect(self):
		ClientProc.Connect(self)

	def Close(self):
		ClientProc.Close(self)

	def SendCMD(self, msg, multiLine = False):
		return ClientProc.SendCMD(self, msg, multiLine)


def backend_send(table_name, table_key, table_partition, src_ip, dst_ip):

	cmd = 'SEND %s,%s,%s,%s,%s\r\n' % (table_name, table_key, table_partition, src_ip, dst_ip)

	c = Client(src_ip, 9999)
	c.Connect()
	ret_message = c.SendCMD(cmd)
	c.Close()

	return ret_message

def add_ldld(table_name, table_key, table_partition, src_ip, dst_ip):
	file_path = IRISFileSystem.exists(table_name, table_key, table_partition)

	if file_path == None:
		part_yyyy = table_partition[:4]
		part_mm = table_partition[4:6]
		part_dd = table_partition[6:8]
		base_path = "%s/%s/%s/%s/%s/%s_%s_%s.DAT" % (table_name, table_key, part_yyyy, part_mm, part_dd, table_name, table_key, table_partition)

		slave_ram_dir = "%s/%s" % (Default.M6_SLAVE_DATA_DIR, base_path)
		slave_disk_dir = "%s/%s" % (Default.M6_SLAVE_DATA_DISK_DIR, base_path)

		if len(glob.glob(slave_ram_dir)) != 0:
			file_path = slave_ram_dir[:-1]  + '%s_%s_%s.DAT' % (table_name, table_key, table_partition)
		elif len(glob.glob(disk_ram_dir)) != 0:
			file_path = slave_disk_dir[:-1]  + '%s_%s_%s.DAT' % (table_name, table_key, table_partition)
			
		if len(glob.glob(slave_ram_dir)) == 0 and len(glob.glob(slave_dir_dir)) == 0:
			ret_message = ["-ERR No Backend %s, %s, %s\r\n" % (table_name, table_key, table_partition)]
			return ret_message


	if 'slave_ssd' in file_path:
		target = 'SSD'
	elif 'slave_disk' in file_path:
		target = 'HDD'
	else:
		target = 'RAM'

	cmd = 'LDLDADD %s,%s,%s,%s,%s,%s\r\n' % (table_name, table_key, table_partition, src_ip, dst_ip, target)

	c = Client(dst_ip, 9999)
	c.Connect()
	ret_message = c.SendCMD(cmd)
	c.Close()

	return ret_message

def del_ldld(table_name, table_key, table_partition, src_ip, dst_ip):
	cmd = 'LDLDDEL %s,%s,%s,%s,%s\r\n' % (table_name, table_key, table_partition, src_ip, dst_ip)

	c = Client(src_ip, 9999)
	c.Connect()
	ret_message = c.SendCMD(cmd)
	c.Close()

	return ret_message

def add_dld(table_name, table_key, table_partition, src_ip, dst_ip):
	master_ip = Default.M6_MASTER_IP_ADDRESS

	cmd = 'DLDADD %s,%s,%s,%s,%s\r\n' % (table_name, table_key, table_partition, src_ip, dst_ip)

	c = Client(master_ip, 9999)
	c.Connect()
	ret_message = c.SendCMD(cmd)
	c.Close()

	return ret_message

def del_dld(table_name, table_key, table_partition, src_ip, dst_ip):
	master_ip = Default.M6_MASTER_IP_ADDRESS

	cmd = 'DLDDEL %s,%s,%s,%s,%s\r\n' % (table_name, table_key, table_partition, src_ip, dst_ip)

	c = Client(master_ip, 9999)
	c.Connect()
	ret_message = c.SendCMD(cmd)
	c.Close()

	return ret_message

def test_dld(table_name, table_key, table_partition, src_ip, dst_ip):
	ret_message = "+OK DLDTEST SUCCESS \r\n"
	master_ip = Default.M6_MASTER_IP_ADDRESS

	p_datetime = datetime.datetime.strptime(table_partition, '%Y%m%d%H%M%S')
	p_endtime = p_datetime + datetime.timedelta(days=1)
	p_starttime = p_datetime - datetime.timedelta(days=1)
	partition_end = datetime.datetime.strftime(p_endtime, '%Y%m%d%H%M%S')
	partition_start = datetime.datetime.strftime(p_starttime, '%Y%m%d%H%M%S')

	partition_range_str = "l.TABLE_PARTITION >= '%s' AND l.TABLE_PARTITION <= '%s'" % (partition_start, partition_end)
	#print partition_range_str
	try:
		with DLDClient(Default.M6_MASTER_IP_ADDRESS, Default.PORT["DLD"]) as client:
			msg = client.FIND_NODE_SELECT(table_name, partition_range_str)
	except Exception, err:
		ret_message = "-ERR DLDTEST %s\r\n" % str(err)

	if msg[0][0] != "+":
		ret_messgae = "-ERR DLDTEST fail\r\n"

	if find_dld_match(msg, dst_ip, table_key, table_partition) == False:
		ret_message = "-ERR DLDTEST backend data is polluted.\r\n"

	return [ret_message]

def find_dld_match(dld_result_list, dst_ip, table_key, table_partition):
	rtn = False
	#print dld_result_list
	for backendInfo in dld_result_list:
		itemList = backendInfo.strip().split(",")
		if len(itemList) == 4:
			backendLocation = itemList[0]
			backendKey = itemList[2]
			backendPartition = itemList[3]

			if backendLocation == dst_ip and backendKey == table_key and backendPartition == table_partition:
				rtn = True
	return rtn


def del_backend(table_name, table_key, table_partition, src_ip, dst_ip):

	cmd = 'DELETE %s,%s,%s,%s,%s\r\n' % (table_name, table_key, table_partition, src_ip, dst_ip)

	c = Client(src_ip, 9999)
	c.Connect()
	ret_message = c.SendCMD(cmd)
	c.Close()

	return ret_message

def dsd_test(table_name, table_key, table_partition, src_ip, dst_ip):
	dsd_port =  Default.PORT['DSD']

	cmd = 'DSDTEST %s,%s,%s,%s,%s\r\n' % (table_name, table_key, table_partition, src_ip, dst_ip)

	c = Client(dst_ip, 9999)
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
			table_name = paramList[0].upper()
			table_key = paramList[1]
			table_partition = paramList[2]
			src_ip = paramList[3]
			dst_ip = paramList[4]

			if my_ip == src_ip:
				
				print "---"
				print paramList
			
				# recovery process	
				#ret_message = del_backend(table_name, table_key, table_partition, dst_ip, src_ip)
				#print ret_message
				#ret_message = del_ldld(table_name, table_key,table_partition, dst_ip, src_ip)
				#print ret_message
				#ret_message = del_dld(table_name, table_key, table_partition, dst_ip, src_ip)
				#print ret_message
				#ret_message = add_dld(table_name, table_key, table_partition, dst_ip, src_ip)
				#print ret_message
				#ret_message = add_ldld(table_name, table_key, table_partition, dst_ip, src_ip)
				#print ret_message
				#ret_message = backend_send(table_name, table_key, table_partition, dst_ip, src_ip)
				#print ret_message
				#ret_message = del_backend(table_name, table_key, table_partition, dst_ip, src_ip)
				#print ret_message
				
			
				ret_message = backend_send(table_name, table_key, table_partition, src_ip, dst_ip)
				print 'SEND : ',ret_message
				if ret_message[0][0] == '-':
					print 'SEND : ',ret_message
					paramList.append('SEND')
					fail_list.append(paramList)
					continue
			   
			   
				ret_message = add_ldld(table_name, table_key, table_partition, src_ip, dst_ip)
				print 'ADD_LDLD : ',ret_message
				if ret_message[0][0] == '-':
					print 'ADD_LDLD : ',ret_message
					paramList.append('ADD_LDLD')
					fail_list.append(paramList)
					continue
			 
				ret_message = add_dld(table_name, table_key, table_partition, src_ip, dst_ip)
				print 'ADD_DLD : ',ret_message
				if ret_message[0][0] == '-':
					print 'ADD_DLD : ',ret_message
					paramList.append('ADD_DLD')
					fail_list.append(paramList)
					continue

				ret_message = del_dld(table_name, table_key, table_partition, src_ip, dst_ip)
				print 'DEL_DLD : ',ret_message
				if ret_message[0][0] == '-':
					print 'DEL_DLD : ',ret_message
					paramList.append('DEL_DLD')
					fail_list.append(paramList)
					continue

				ret_message = del_ldld(table_name, table_key, table_partition, src_ip, dst_ip)
				print 'DEL_LDLD : ',ret_message
				if ret_message[0][0] == '-':
					print 'DEL_LDLD : ',ret_message
					paramList.append('DEL_LDLD')
					fail_list.append(paramList)
					continue
			
				ret_message = dsd_test(table_name, table_key, table_partition, src_ip, dst_ip)
				print 'DSD_TEST : ',ret_message
				if ret_message[0][0] == '-':
					print 'DSD_TEST : ',ret_message
					paramList.append('DSD_TEST')
					fail_list.append(paramList)
					continue

				ret_message = test_dld(table_name, table_key, table_partition, src_ip, dst_ip)
				print 'DLD_TEST : ',ret_message
				if ret_message[0][0] == '-':
					print 'DLD_TEST : ',ret_message
					paramList.append('DLD_TEST')
					fail_list.append(paramList)
					continue

				ret_message = del_backend(table_name, table_key, table_partition, src_ip, dst_ip)
				print 'DEL_BACKEND : ',ret_message
				if ret_message[0][0] == '-':
					print 'DEL_BACKEND : ',ret_message
					paramList.append('DEL_BACKEND')
					fail_list.append(paramList)
					continue


	with open('./migration_recovery_info.dat', 'w') as rf:
		for recovery_info in fail_list:
			recovery_str = ','.join(recovery_info)
			recovery_str += '\n'
			rf.write(recovery_str)
			
