import sys
import pathlib

import zmq
import molsim_job_scheduler as mjs

assert len(sys.argv) == 2

qsub_file = pathlib.Path(sys.argv[1])
assert qsub_file.exists()

nodes = mjs.extract_nodes_from_qsub(qsub_file)
assert nodes is not None

# Check nodes format.
mjs.parse_nodes(nodes)

context = zmq.Context()

#  Socket to talk to server
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:{}".format(mjs.JobManipulator.PORT))

# message = "qas|qsubfile"
message = "qas|" + str(qsub_file.resolve())
socket.send(message.encode())
print(qsub_file)
print(socket.recv().decode("utf-8"))
