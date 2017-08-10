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

if __name__ == "__main__":
	print "start"
	c = Client("localhost", 9999)
	print "init"
	print c.Connect()
	print "connect"
	print c.SendCMD("SEND 192.168.111.221:/home/iris/ttt:/home/iris/ttt\r\n")
	print "send CMD"
	c.Close()
