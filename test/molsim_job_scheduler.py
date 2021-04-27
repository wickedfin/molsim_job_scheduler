import os
import time
import atexit
import pathlib
import threading
import subprocess
import collections
from xml.etree import ElementTree

import zmq

QSTAT_PATH = "/usr/local/pbs/bin/qstat"

STAT_DATA_PATH = str(pathlib.Path(__file__).parent / "stat.dat")
JOBS_DATA_PATH = str(pathlib.Path(__file__).parent / "jobs.dat")

Stat = collections.namedtuple("Stat",
          [
              "user",
              "nodes",
              "start_time",
              "end_time",
              "last_update_time",
              "duration",
          ]
       )


def print_xml(elem, level=0):
    for c in elem:
        print("    "*level, c.tag, c.text)
        print_xml(c, level+1)


def extract_nodes_from_qsub(qsub):
    with open(qsub, "r") as f:
        for line in f:
            if not line.startswith("#PBS -l"):
                continue
            # [:-1] to remove "\n", [5:] to remove "nodes=".
            return line[:-1].split()[-1][6:]
    return None


def parse_nodes(nodes):
    """
    Example of nodes: "1:ppn=8:ac"
    """
    tokens = nodes.split(":")
    assert len(tokens) == 3

    n_cores = int(tokens[0]) * int(tokens[1].split("=")[1])
    node_name = tokens[2]

    return node_name, n_cores


def read_stat():
    # Get previous stat.
    stat_data = {}
    stat_path = pathlib.Path(STAT_DATA_PATH)
    if stat_path.exists():
        with stat_path.open("r") as f:
            for line in f:
                # [-1] for duration.
                tokens = line[:-1].split("|")
                stat = Stat._make(tokens[1:] + [-1])
                stat_data[tokens[0]] = stat
    # Alias.
    stat_days = StatParser.STAT_DAYS
    stat_seconds = stat_days * 24 * 3600
    current_time = time.time()
    stat_start_time = current_time - stat_seconds

    # Calculate duration.
    for k in stat_data.keys():
        start_time = stat_data[k].start_time
        if start_time == "-1":
            continue

        end_time = stat_data[k].end_time

        start_time = max(int(start_time), stat_start_time)

        if end_time == "-1":
            end_time = current_time
        else:
            end_time = float(end_time)

        duration = end_time - start_time          

        stat_data[k] = stat_data[k]._replace(duration=duration)

    return stat_data


def write_stat(stat_data):
    # Update stat file.
    stat_path = pathlib.Path(STAT_DATA_PATH)
    with stat_path.open("w") as f:
        for k, stat in stat_data.items():
            # Duration is not saved ([:-1]).
            v = list(stat._asdict().values())[:-1]
            f.write("{}|{}|{}|{}|{}|{}\n".format(k, *v))


class StatParser:
    """
    StatParser.parse() returns stat_data.

    stat_data
        key: job id
        value: Stat

        time = second unit
    """
    # Day unit.
    DATA_STORAGE_PERIOD = 30
    STAT_DAYS = 14

    def parse(self):
        # Alias.
        stat_days = StatParser.STAT_DAYS
        stat_seconds = stat_days * 24 * 3600
        # Stat storage period.
        storage_seconds = StatParser.DATA_STORAGE_PERIOD * 24 * 3600
        current_time = time.time()
        stat_start_time = current_time - stat_seconds

        # Get qstat information.
        result = subprocess.run(
            [QSTAT_PATH, "-xf"],
            stdout=subprocess.PIPE
        )
        root = ElementTree.fromstring(result.stdout.decode("utf-8"))

        """
        # Get previous stat.
        stat_data = {}
        stat_path = pathlib.Path("stat.dat")
        if stat_path.exists():
            with stat_path.open("r") as f:
                for line in f:
                    # [-1] for duration.
                    tokens = line[:-1].split("|")
                    stat = Stat._make(tokens[1:] + [-1])
                    stat_data[tokens[0]] = stat
        """
        stat_data = read_stat()

        # Get current stat.       
        data = ""
        for e in root.findall("Job"):
            c = e.find("job_state")
            #if c.text != "R":
            #    continue

            c = e.find("Job_Owner")
            user = c.text.split("@")[0]

            c = e.find("Job_Id")
            job_id = c.text

            c = e.find("start_time")
            if c is None:
                start_time = "-1"
            else:
                start_time = int(c.text)

            c = e.find("Resource_List").find("nodes")
            nodes = c.text

            end_time = "-1"
            data += "{}|{}|{}|{}|{}|{}\n".format(
                job_id, user, nodes, start_time, end_time, current_time
            )

        temp_data = {}
        for line in data.split("\n")[:-1]:
            tokens = line.split("|")
            # [-1] for duration.
            stat = Stat._make(tokens[1:] + [-1])
            temp_data[tokens[0]] = stat

        # Update Q -> R jobs.
        for k in stat_data.keys():
            if k not in temp_data:
                continue
            if stat_data[k].start_time != "-1":
                continue
            if temp_data[k].start_time == "-1":
                continue

            stat_data[k] = temp_data[k]

        # Set end time of ended jobs.
        ended_jobs = []
        for k in stat_data.keys():
            if k in temp_data:
                continue

            if stat_data[k].end_time != "-1":
                continue

            ended_jobs.append(k)

        for job_id in ended_jobs:
            stat_data[job_id] = stat_data[job_id]._replace(
                end_time=stat_data[job_id].last_update_time
            )

        # Update stat data.
        for k, v in temp_data.items():
            if k in stat_data:
                continue
            stat_data[k] = v

        # Remode old jobs.
        old_jobs = []
        old_time = current_time - storage_seconds
        for k, v in stat_data.items():
            if v.end_time == "-1":
                continue

            end_time = float(v.end_time)
            if end_time < old_time:
                old_jobs.append(k)

        for k in old_jobs:
            stat_data.pop(k, None)

        # Calculate duration.
        for k in stat_data.keys():
            start_time = stat_data[k].start_time
            if start_time == "-1":
                continue

            end_time = stat_data[k].end_time

            start_time = max(int(start_time), stat_start_time)

            if end_time == "-1":
                end_time = current_time
            else:
                end_time = float(end_time)

            duration = end_time - start_time          

            stat_data[k] = stat_data[k]._replace(duration=duration)

        # Update last_update_time.
        for k in stat_data.keys():
            stat_data[k] = stat_data[k]._replace(last_update_time=current_time)

        """
        # Update stat file.
        with stat_path.open("w") as f:
            for k, stat in stat_data.items():
                # Duration is not saved ([:-1]).
                v = list(stat._asdict().values())[:-1]
                f.write("{}|{}|{}|{}|{}|{}\n".format(k, *v))
        """
        write_stat(stat_data)

        return stat_data


def calculate_usage(stat_data):
    defaultdict = collections.defaultdict
    usage = defaultdict(lambda: defaultdict(int))
    for k, v in stat_data.items():
        user = v.user

        # N cores.
        nodes = v.nodes
        try:
            node_name, n_cores = parse_nodes(nodes)
        except:
            node_name, n_cores = "error", 1

        duration = v.duration
        if duration == "-1":
            continue

        usage[user][node_name] += n_cores * duration
    
    return usage


def calculate_running_cores(stat_data):
    defaultdict = collections.defaultdict
    running_cores = defaultdict(lambda: defaultdict(int))
    for k, v in stat_data.items():
        if v.end_time != "-1":
            continue

        user = v.user

        # N cores.
        nodes = v.nodes
        try:
            node_name, n_cores = parse_nodes(nodes)
        except:
            node_name, n_cores = "error", 1
        running_cores[user][node_name] += n_cores
    
    return running_cores


def print_usage(usage):
    for k, v in usage.items():
        print("="*20)
        print("User:", k, "\n")
        print("{:10s}{:10s}".format("Node", "Usage (sec)"))
        for kk, vv in v.items():
            print("{:10s}{:<10.0f}".format(kk, vv))


class Job:
    def __init__(self, _id, _dir, _file, _time, nodes, user):
        self.id = _id
        self.dir = str(pathlib.Path(_dir).resolve())
        self.file = _file
        # Submission time.
        self.time = _time
        self.nodes = nodes
        self.user = user

        self.submitted = False

    def submit(self):
        su_command = 'cd {}; qsub {}'.format(self.dir, self.file)
        result = subprocess.run(
            ["su", "-", self.user, "-c", su_command]
        )
        print(self.id, self.user, self.nodes, "submitted")
        self.submitted = True

    @staticmethod
    def from_string(string):
        if string[-1] == "\n":
            string = string[:-1]
        tokens = string.split("|")
        
        job = Job(*tokens)
        # Type cast.
        job.id = int(job.id)
        job.time = float(job.time)

        return job

    def to_string(self):
        return "{}|{}|{}|{}|{}|{}".format(
            self.id, self.dir, self.file, self.time, self.nodes, self.user
        )

    def __repr__(self):
        return self.to_string()


# Global variables.
MAX_ID = 0
JOBS = []
STAT_DATA = {}
LOCK = threading.Lock()


def save_jobs(jobs):
    """
    Usage:
        LOCK.acquire()
        save_jobs(jobs)
        LOCK.release()
    """
    with open(JOBS_DATA_PATH, "w") as f:
        for job in jobs:
            f.write(job.to_string()+"\n")


class Scheduler(threading.Thread):
    UPDATE_INTERVAL = 1

    def __init__(self, parser):
        super().__init__()
        self.config = self._parse_config()
        self.parser = parser

    def _parse_config(self):
        config = {}
        with open("config.txt", "r") as f:
            for line in f.readlines():
                if line.startswith("#"):
                    continue
                node_name, cores, limits = line.split()
                config[node_name] = int(cores), int(limits)
        return config

    def run(self):
        global JOBS

        while True:
            start_time = time.time()
            # -------------------------------------------
            LOCK.acquire()
            STAT_DATA = self.parser.parse()
            usage_dict = calculate_usage(STAT_DATA)
            running_cores = calculate_running_cores(STAT_DATA)

            # Sort by (node_name, usage, submission_time).
            key_jobs = []
            for job in JOBS:
                node_name = job.nodes.split(":")[2]
                usage = usage_dict[job.user][node_name]
                key = (node_name, usage, job.time)
                key_jobs.append((key, job))

            sorted_key_jobs = sorted(key_jobs, key=lambda x: x[0])
            for key, job in sorted_key_jobs:
                try:
                    node_name, n_cores = parse_nodes(nodes)
                except:
                    node_name, n_cores = "error", 1

                # Neglect invalid node names.
                if node_name not in self.config:
                    continue

                limits = self.config[node_name][1]
                if running_cores[job.user][node_name] + n_cores > limits:
                    continue

                all_cores = sum([v[node_name] for v in running_cores.values()])
                max_cores = self.config[node_name][0]
                if all_cores + n_cores > max_cores:
                    continue

                job.submit()
                running_cores[job.user][node_name] += n_cores

            JOBS = [job for _, job in sorted_key_jobs if not job.submitted]
            save_jobs(JOBS)

            LOCK.release()

            # ------------------------------------------
            duration = time.time() - start_time
            time.sleep(max(Scheduler.UPDATE_INTERVAL - duration, 1))


def read_jobs():
    jobs_path = pathlib.Path(JOBS_DATA_PATH)
    jobs = []
    if jobs_path.exists():
        with jobs_path.open("r") as f:
            for line in f:
                job = Job.from_string(line)
                jobs.append(job)
    return jobs


class JobManipulator(threading.Thread):
    PORT = 55554
    def __init__(self):
        super().__init__()
        self.max_id = 0

    def run(self):
        global JOBS

        LOCK.acquire()
        # Load backup jobs.
        JOBS = read_jobs()
        LOCK.release()

        # Set max id.
        self.max_id = 0
        if JOBS:
            self.max_id = max([job.id for job in JOBS])

        # Prepair communications.
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:{}".format(JobManipulator.PORT))

        while True:
            #  Wait for next request from client
            message = socket.recv().decode("utf-8")

            tokens = message.split("|")
            function = tokens[0]

            if function == "qas":
                self.do_qas(tokens[1:])
            elif function == "qrm":
                self.do_qrm(tokens[1:])

            socket.send(b"Done")

    def do_qas(self, args):
        global JOBS

        LOCK.acquire()
        for qsub in args:
            JOBS.append(self.job_from_qsub(qsub))
        LOCK.release()

    def do_qrm(self, args):
        global JOBS

        user = args[0]
        ids = set([int(v) for v in args[1:]])

        LOCK.acquire()
        JOBS = [job for job in JOBS 
            if not (job.user == user and (job.id in ids))
        ]           
        LOCK.release()

    def job_from_qsub(self, qsub):
        path = pathlib.Path(qsub)
        self.max_id += 1
        _id = self.max_id
        _dir = path.parent
        _file = path.name
        _time = time.time()
        nodes = extract_nodes_from_qsub(path)
        user = path.parts[3]        

        return Job(
            _id=_id, _dir=_dir, _file=_file,
            _time=_time, nodes=nodes, user=user
        )


def main():
    @atexit.register
    def print_jobs():
        print("BACKUP JOBS.")
        save_jobs(JOBS)
        print("BACKUP DONE.")

    parser = StatParser()
    usage = calculate_usage(parser.parse())

    job_manipulator = JobManipulator()
    scheduler = Scheduler(parser=parser)

    job_manipulator.start()
    scheduler.start()

    job_manipulator.join()
    scheduler.join()


if __name__ == "__main__":
    main()
