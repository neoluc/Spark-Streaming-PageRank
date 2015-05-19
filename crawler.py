import sys
import socket
import time
from random import randint

if __name__ == "__main__":
	soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	soc.bind(("localhost", 9999))
	soc.listen(5)
	print "waiting for connection"
	(csoc, addr) = soc.accept()
	print "client connected"
	while True:
		a = randint(0, 19)
		b = randint(0, 19)
		if a == b:
			continue
		s = `a` + ' ' + `b`
		print s
		csoc.sendall(s + '\n')
		time.sleep(2)