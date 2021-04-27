import sys
import getpass
import pathlib

import zmq
import molsim_job_scheduler as mjs

user = getpass.getuser()

context = zmq.Context()

#  Socket to talk to server
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:{}".format(mjs.JobManipulator.PORT))

# message = "qrm|id1|id2|id2|..."
# str(int(x)): check x is int
message = "qrm|" + "|".join([user] + [str(int(v)) for v in sys.argv[1:]])
socket.send(message.encode())
print(sys.argv[1:])
print(socket.recv().decode("utf-8"))
